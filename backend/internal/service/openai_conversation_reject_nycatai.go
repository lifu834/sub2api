package service

// NYCATAI patch (conversation-reject-failover, 261124): 号对"这个对话"不可用时换号, 并在会话内避开该号。
//
// 背景(261124 codex_max): 一些上游会按对话拒绝, 而不是号坏了:
//   - sourcesdata / biuapi g138 的网关对话锁: HTTP 400 conversation_safety_locked「当前对话因上游安全拒绝已锁定」,
//     锁只存在于这类站点, 同一对话在别家能照常续写(请求根本没到 OpenAI);
//   - mdkj g171 对部分长对话回 HTTP 400 invalid_request "The request could not be processed. Please check the
//     request parameters."(同一对话在它上面先成功后持续失败, 疑似超长限制), 别家可能接得住。
// 原逻辑把 400 当客户端错误直接透传, 不换号; 粘性还留在该号上, 客户重试会反复撞同一个号。
//
// 开关 nycatai_conversation_reject_failover = off | on (settings 表, 与其他 NYCATAI 键同一 5 秒缓存, 默认 off):
//  1. 上述两类 HTTP 400 在写出前换号(排除当前号), 不计号的调度 / 健康失败, 不触发号的副作用(停用 / 冷却);
//     与加密不匹配换号共用单请求上限 nycataiEncryptedMismatchMaxSwitches, 耗尽按 400 回上游原文。
//  2. 会话避号: 这类换号(以及加密不匹配换号)记下"(分组, 会话) 不要再选这个号" nycataiConversationAvoidTTL;
//     选号时把它们并入排除集; 并入后一个号都选不出时退回原排除集(宁可撞原错误, 也不变成"无可用号")。
//     记录只在进程内存里(生产单实例), 重启丢失可接受。
// 不处理: OpenAI 首次安全拒绝(invalid_prompt, 流内, 站方带 retry:stop)——被拒内容不转给别家重放。

import (
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	SettingKeyNycataiConversationRejectFailover = "nycatai_conversation_reject_failover"

	// GatewayFailureReasonNycataiConversationReject 标记"该号拒绝这个对话"引起的换号。
	GatewayFailureReasonNycataiConversationReject GatewayFailureReason = "nycatai_conversation_reject"

	nycataiConversationRejectSafetyLock     = "safety_lock"
	nycataiConversationRejectGenericInvalid = "generic_invalid_request"

	nycataiConversationAvoidTTL        = 24 * time.Hour
	nycataiConversationAvoidMaxEntries = 100000
)

var nycataiConversationRejectFailoverOn atomic.Bool

// NycataiConversationRejectFailoverEnabled 开关当前值(随 nycataiSchedulerSettings 刷新)。
func NycataiConversationRejectFailoverEnabled() bool {
	return nycataiConversationRejectFailoverOn.Load()
}

// nycataiConversationScopedEnabled 任一"按对话换号"开关打开时, 会话避号生效。
func nycataiConversationScopedEnabled() bool {
	return NycataiEncryptedMismatchFailoverEnabled() || NycataiConversationRejectFailoverEnabled()
}

// nycataiConversationRejectKind 识别"该号拒绝这个对话"的 HTTP 错误, 返回类别(不是则 "")。只认 400 和精确特征。
func nycataiConversationRejectKind(statusCode int, upstreamCode, upstreamMsg string, upstreamBody []byte) string {
	if statusCode != http.StatusBadRequest {
		return ""
	}
	code := strings.ToLower(strings.TrimSpace(upstreamCode))
	if code == "" {
		code = strings.ToLower(strings.TrimSpace(extractUpstreamErrorCode(upstreamBody)))
	}
	msg := strings.TrimSpace(upstreamMsg)
	if msg == "" {
		msg = strings.TrimSpace(extractUpstreamErrorMessage(upstreamBody))
	}
	lower := strings.ToLower(msg)
	if code == "conversation_safety_locked" || strings.Contains(msg, "当前对话因上游安全拒绝已锁定") {
		return nycataiConversationRejectSafetyLock
	}
	if code == "invalid_request" && strings.TrimSuffix(lower, ".") == "the request could not be processed. please check the request parameters" {
		return nycataiConversationRejectGenericInvalid
	}
	return ""
}

// IsNycataiConversationReject 换号错误是否由"该号拒绝这个对话"引起。
func (e *UpstreamFailoverError) IsNycataiConversationReject() bool {
	return e != nil && e.Reason == GatewayFailureReasonNycataiConversationReject
}

// IsNycataiConversationScoped 换号是否按对话(加密不匹配 / 对话拒绝): 不计号的失败, 共用换号上限, 耗尽回 400 原文。
func (e *UpstreamFailoverError) IsNycataiConversationScoped() bool {
	return e.IsNycataiEncryptedMismatch() || e.IsNycataiConversationReject()
}

// nycataiConversationRejectFailover 开关打开且识别为对话拒绝时, 记运维事件并返回换号错误; 否则 nil。
// 不调用 handleFailoverSideEffects: 这不是号的问题, 不能停用 / 冷却该号。
func (s *OpenAIGatewayService) nycataiConversationRejectFailover(
	c *gin.Context,
	account *Account,
	statusCode int,
	headers http.Header,
	upstreamCode, upstreamMsg string,
	upstreamBody []byte,
) *UpstreamFailoverError {
	if !NycataiConversationRejectFailoverEnabled() || account == nil {
		return nil
	}
	kind := nycataiConversationRejectKind(statusCode, upstreamCode, upstreamMsg, upstreamBody)
	if kind == "" {
		return nil
	}
	detail := ""
	if s != nil && s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		detail = truncateString(string(upstreamBody), maxBytes)
	}
	requestID := ""
	if headers != nil {
		requestID = headers.Get("x-request-id")
	}
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		ProxyID:            opsUpstreamProxyID(account),
		ProxyName:          opsUpstreamProxyName(account),
		Platform:           account.Platform,
		AccountID:          account.ID,
		AccountName:        account.Name,
		UpstreamStatusCode: statusCode,
		UpstreamRequestID:  requestID,
		Kind:               "failover",
		Message:            upstreamMsg,
		Detail:             detail,
	})
	message := strings.TrimSpace(upstreamMsg)
	if message == "" {
		message = strings.TrimSpace(extractUpstreamErrorMessage(upstreamBody))
	}
	var clonedHeaders http.Header
	if headers != nil {
		clonedHeaders = headers.Clone()
	}
	slog.Info("nycatai_conversation_reject_detected", "account_id", account.ID, "kind", kind, "status", statusCode)
	return &UpstreamFailoverError{
		StatusCode:             statusCode,
		ResponseBody:           upstreamBody,
		ResponseHeaders:        clonedHeaders,
		Stage:                  GatewayFailureStageInference,
		Scope:                  GatewayFailureScopeRequest,
		Reason:                 GatewayFailureReasonNycataiConversationReject,
		RetryableOnSameAccount: false,
		RequestScopedTransient: false,
		NextAccountAction:      NextAccountRetry,
		ClientStatusCode:       http.StatusBadRequest,
		ClientMessage:          message,
	}
}

// ---- 会话避号 ----

var (
	nycataiConversationAvoidMu sync.Mutex
	nycataiConversationAvoid   = map[string]map[int64]int64{} // "gid:session" -> accountID -> expiresAt(ns)
)

func resetNycataiConversationAvoidForTest() {
	nycataiConversationAvoidMu.Lock()
	nycataiConversationAvoid = map[string]map[int64]int64{}
	nycataiConversationAvoidMu.Unlock()
	nycataiConversationRejectFailoverOn.Store(false)
}

func sweepNycataiConversationAvoidLocked(now int64) {
	for key, accounts := range nycataiConversationAvoid {
		for id, exp := range accounts {
			if now >= exp {
				delete(accounts, id)
			}
		}
		if len(accounts) == 0 {
			delete(nycataiConversationAvoid, key)
		}
	}
}

// NycataiAvoidAccountForConversation 记下"这个会话不要再选 accountID"(按对话换号后调用)。
func (s *OpenAIGatewayService) NycataiAvoidAccountForConversation(groupID *int64, sessionHash string, accountID int64) {
	sessionHash = strings.TrimSpace(sessionHash)
	if !nycataiConversationScopedEnabled() || sessionHash == "" || accountID <= 0 {
		return
	}
	now := nycataiNow().UnixNano()
	key := nycataiLineagePinKey(groupID, sessionHash)
	nycataiConversationAvoidMu.Lock()
	defer nycataiConversationAvoidMu.Unlock()
	accounts, exists := nycataiConversationAvoid[key]
	if !exists {
		if len(nycataiConversationAvoid) >= nycataiConversationAvoidMaxEntries {
			sweepNycataiConversationAvoidLocked(now)
			if len(nycataiConversationAvoid) >= nycataiConversationAvoidMaxEntries {
				slog.Warn("nycatai_conversation_avoid_full", "entries", len(nycataiConversationAvoid))
				return
			}
		}
		accounts = map[int64]int64{}
		nycataiConversationAvoid[key] = accounts
	}
	accounts[accountID] = now + int64(nycataiConversationAvoidTTL)
}

// nycataiConversationAvoidedAccounts 返回会话当前要避开的号(开关关 / 没有 = nil)。
func nycataiConversationAvoidedAccounts(groupID *int64, sessionHash string) map[int64]struct{} {
	sessionHash = strings.TrimSpace(sessionHash)
	if !nycataiConversationScopedEnabled() || sessionHash == "" {
		return nil
	}
	now := nycataiNow().UnixNano()
	key := nycataiLineagePinKey(groupID, sessionHash)
	nycataiConversationAvoidMu.Lock()
	defer nycataiConversationAvoidMu.Unlock()
	accounts, ok := nycataiConversationAvoid[key]
	if !ok {
		return nil
	}
	out := make(map[int64]struct{}, len(accounts))
	for id, exp := range accounts {
		if now >= exp {
			delete(accounts, id)
			continue
		}
		out[id] = struct{}{}
	}
	if len(accounts) == 0 {
		delete(nycataiConversationAvoid, key)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// nycataiMergeConversationAvoid 把会话避号并入排除集; 返回新集合与是否真的多排除了号(没有新增时返回原集合, false)。
func nycataiMergeConversationAvoid(groupID *int64, sessionHash string, excludedIDs map[int64]struct{}) (map[int64]struct{}, bool) {
	avoid := nycataiConversationAvoidedAccounts(groupID, sessionHash)
	if len(avoid) == 0 {
		return excludedIDs, false
	}
	merged := cloneExcludedAccountIDs(excludedIDs)
	if merged == nil {
		merged = make(map[int64]struct{}, len(avoid))
	}
	added := false
	for id := range avoid {
		if _, ok := merged[id]; !ok {
			merged[id] = struct{}{}
			added = true
		}
	}
	if !added {
		return excludedIDs, false
	}
	return merged, true
}
