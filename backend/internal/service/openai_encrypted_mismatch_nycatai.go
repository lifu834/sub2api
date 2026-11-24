package service

// NYCATAI patch (encrypted-mismatch-failover, 261124): 对话跨上游后"加密上下文解不开"时换号找回原号。
//
// 背景: Codex 对话里带有上游生成的加密内容(推理项 / 加密函数输出 / 续写上下文), 只有生成它的那家
// (或与之兼容的上游)解得开。对话因粘性过期、健康逃逸或故障转移落到不兼容的号上时, 上游在流里回
// response.failed(invalid_encrypted_content / mdkj 的 encrypted_context_invalid 等), 原逻辑把它当
// invalid_request 直接透传给客户, 还计入该号错误率, 进而触发别的会话逃逸(261124 codex_max 实例)。
//
// 开关 nycatai_encrypted_mismatch_failover = off | on (settings 表, 与其他 NYCATAI 键同一 5 秒缓存, 默认 off):
//  1. 流内 failed/error 事件识别为"加密上下文不匹配"且还没向客户写出语义内容时, 走换号(排除已试过的号),
//     单请求最多 nycataiEncryptedMismatchMaxSwitches 次; 全部失败才把原错误按 400 返回客户。
//  2. 这类换号不计入号的调度失败 / 健康失败(不是号坏了, 是选错了号)。
//  3. 换号后在某号上成功 = 找到了能解开的号, 把(分组, 会话)钉在该号 nycataiLineagePinTTL:
//     期间该会话不做健康逃逸, 粘性过期后也优先回到该号; 号不可调度 / 不兼容时照常换走。
//     钉住记录只在进程内存里(生产单实例), 重启丢失可接受(丢了只是回到第 1 步自愈)。

import (
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
)

const (
	SettingKeyNycataiEncryptedMismatchFailover = "nycatai_encrypted_mismatch_failover"

	// GatewayFailureReasonNycataiEncryptedMismatch 标记"加密上下文与所选号不匹配"引起的换号。
	GatewayFailureReasonNycataiEncryptedMismatch GatewayFailureReason = "nycatai_encrypted_context_mismatch"

	// 单请求因加密不匹配最多换号次数(另受全局 max_account_switches 约束)。
	nycataiEncryptedMismatchMaxSwitches = 5

	nycataiLineagePinTTL        = 24 * time.Hour
	nycataiLineagePinMaxEntries = 100000
)

var nycataiEncryptedMismatchFailoverOn atomic.Bool

func parseNycataiOnOff(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "true", "1", "enforce", "enabled":
		return true
	default:
		return false
	}
}

// NycataiEncryptedMismatchFailoverEnabled 开关当前值(随 nycataiSchedulerSettings 刷新)。
func NycataiEncryptedMismatchFailoverEnabled() bool {
	return nycataiEncryptedMismatchFailoverOn.Load()
}

// isNycataiEncryptedContextMismatch 判断流内失败事件是否为"加密上下文解不开"。
// 覆盖: OpenAI invalid_encrypted_content(含 "Encrypted function output content could not be decrypted"),
// mdkj encrypted_context_invalid("could not validate encrypted continuation context"),
// "The encrypted content … could not be verified", 千川脱敏文案 "Upstream encrypted content error"。
func isNycataiEncryptedContextMismatch(payload []byte, message string) bool {
	code := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.error.code").String()))
	if code == "" {
		code = strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "error.code").String()))
	}
	switch code {
	case "invalid_encrypted_content", "encrypted_context_invalid":
		return true
	}
	combined := strings.ToLower(message + " " +
		gjson.GetBytes(payload, "response.error.message").String() + " " +
		gjson.GetBytes(payload, "error.message").String())
	switch {
	case strings.Contains(combined, "could not validate encrypted continuation context"):
		return true
	case strings.Contains(combined, "encrypted function output content could not be decrypted"):
		return true
	case strings.Contains(combined, "encrypted content") && strings.Contains(combined, "could not be verified"):
		return true
	case strings.Contains(combined, "upstream encrypted content error"):
		return true
	}
	return false
}

// nycataiEncryptedMismatchShouldFailover: 开关打开且识别为加密不匹配时返回 true。
func nycataiEncryptedMismatchShouldFailover(payload []byte, message string) bool {
	return NycataiEncryptedMismatchFailoverEnabled() && isNycataiEncryptedContextMismatch(payload, message)
}

// IsNycataiEncryptedMismatch 换号错误是否由加密不匹配引起。
func (e *UpstreamFailoverError) IsNycataiEncryptedMismatch() bool {
	return e != nil && e.Reason == GatewayFailureReasonNycataiEncryptedMismatch
}

// markNycataiEncryptedMismatchFailover 把流内换号错误标成加密不匹配: 不同号重试、不计号的失败, 耗尽时按 400 回原文。
func markNycataiEncryptedMismatchFailover(failoverErr *UpstreamFailoverError, message string) {
	if failoverErr == nil {
		return
	}
	failoverErr.Reason = GatewayFailureReasonNycataiEncryptedMismatch
	failoverErr.Scope = GatewayFailureScopeRequest
	failoverErr.RetryableOnSameAccount = false
	failoverErr.RequestScopedTransient = false
	failoverErr.NextAccountAction = NextAccountRetry
	failoverErr.ClientStatusCode = 400
	failoverErr.ClientMessage = strings.TrimSpace(message)
}

// NycataiEncryptedMismatchSwitchesExhausted 单请求加密不匹配换号次数是否已达上限。
func NycataiEncryptedMismatchSwitchesExhausted(count int) bool {
	return count >= nycataiEncryptedMismatchMaxSwitches
}

// ---- 会话钉号 ----

type nycataiLineagePin struct {
	accountID int64
	expiresAt int64
}

var (
	nycataiLineagePinMu sync.Mutex
	nycataiLineagePins  = map[string]nycataiLineagePin{}
)

func resetNycataiLineagePinsForTest() {
	nycataiLineagePinMu.Lock()
	nycataiLineagePins = map[string]nycataiLineagePin{}
	nycataiLineagePinMu.Unlock()
	nycataiEncryptedMismatchFailoverOn.Store(false)
}

func nycataiLineagePinKey(groupID *int64, sessionHash string) string {
	var gid int64
	if groupID != nil {
		gid = *groupID
	}
	return strconv.FormatInt(gid, 10) + ":" + sessionHash
}

// sweepNycataiLineagePinsLocked 清过期记录; 调用方持锁。
func sweepNycataiLineagePinsLocked(now int64) {
	for key, pin := range nycataiLineagePins {
		if now >= pin.expiresAt {
			delete(nycataiLineagePins, key)
		}
	}
}

// NycataiPinEncryptedLineage 记下"这个会话的加密内容只有 accountID 解得开"。
func (s *OpenAIGatewayService) NycataiPinEncryptedLineage(groupID *int64, sessionHash string, accountID int64) {
	sessionHash = strings.TrimSpace(sessionHash)
	if !NycataiEncryptedMismatchFailoverEnabled() || sessionHash == "" || accountID <= 0 {
		return
	}
	now := nycataiNow().UnixNano()
	key := nycataiLineagePinKey(groupID, sessionHash)
	nycataiLineagePinMu.Lock()
	defer nycataiLineagePinMu.Unlock()
	if _, exists := nycataiLineagePins[key]; !exists && len(nycataiLineagePins) >= nycataiLineagePinMaxEntries {
		sweepNycataiLineagePinsLocked(now)
		if len(nycataiLineagePins) >= nycataiLineagePinMaxEntries {
			slog.Warn("nycatai_encrypted_lineage_pin_full", "entries", len(nycataiLineagePins))
			return
		}
	}
	nycataiLineagePins[key] = nycataiLineagePin{accountID: accountID, expiresAt: now + int64(nycataiLineagePinTTL)}
}

// nycataiEncryptedLineagePinnedAccount 返回会话钉住的号(没有 / 过期 / 开关关 = 0)。
func nycataiEncryptedLineagePinnedAccount(groupID *int64, sessionHash string) int64 {
	sessionHash = strings.TrimSpace(sessionHash)
	if !NycataiEncryptedMismatchFailoverEnabled() || sessionHash == "" {
		return 0
	}
	now := nycataiNow().UnixNano()
	key := nycataiLineagePinKey(groupID, sessionHash)
	nycataiLineagePinMu.Lock()
	defer nycataiLineagePinMu.Unlock()
	pin, ok := nycataiLineagePins[key]
	if !ok {
		return 0
	}
	if now >= pin.expiresAt {
		delete(nycataiLineagePins, key)
		return 0
	}
	return pin.accountID
}

// nycataiEncryptedLineagePinned 会话是否钉在 accountID 上(钉住时不做健康逃逸)。
func nycataiEncryptedLineagePinned(groupID *int64, sessionHash string, accountID int64) bool {
	return accountID > 0 && nycataiEncryptedLineagePinnedAccount(groupID, sessionHash) == accountID
}

// nycataiStickyEscapeBlockedByLineagePin: 会话钉在该号上时不做健康逃逸(逃到别家只会解不开)。
func nycataiStickyEscapeBlockedByLineagePin(groupID *int64, sessionHash string, accountID int64, reason string, errorRate, ttft float64) bool {
	if !nycataiEncryptedLineagePinned(groupID, sessionHash, accountID) {
		return false
	}
	var gid int64
	if groupID != nil {
		gid = *groupID
	}
	slog.Info("sticky_escape_suppressed_encrypted_lineage_pin",
		"account_id", accountID, "group_id", gid, "reason", reason, "error_rate", errorRate, "ttft", ttft)
	return true
}
