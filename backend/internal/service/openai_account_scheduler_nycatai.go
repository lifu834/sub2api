package service

// NYCATAI patch (sticky-escape-guard, 261027): 两处调度补充, 默认全部关闭, 由 settings 表热切换(5 秒缓存, 不用重启)。
//
//  1. 逃逸守门 nycatai_sticky_escape_guard = off | shadow | enforce
//     健康逃逸(首字/错误率超线)前先看同组还有没有"未超线"的候选号。没有时逃逸只会把会话搬到同样超线的号上、
//     白丢缓存(261027 codex_max 4 号全员超线时会话来回改绑, 缓存只剩 4096)。
//     shadow 只打 sticky_escape_no_healthy_target 日志、照常逃逸; enforce 则不逃逸, 会话留在原号。
//  2. 运行时统计衰减(按号的错误率 / 首字 EWMA, 原生无时间衰减, 被逃逸的号拿不到样本就一直冻在超线值,
//     261027 #210 错误率 0.4198 冻了 3 小时以上):
//     nycatai_runtime_stats_error_half_life_seconds: 错误率按距上次结果的时长指数衰减(半衰期, 0 = 关)
//     nycatai_runtime_stats_ttft_stale_seconds: 首字距上次样本超过此时长视为无样本(打分给中性分, 不触发逃逸; 0 = 关)

import (
	"context"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	SettingKeyNycataiStickyEscapeGuard          = "nycatai_sticky_escape_guard"
	SettingKeyNycataiRuntimeStatsErrorHalfLifeS = "nycatai_runtime_stats_error_half_life_seconds"
	SettingKeyNycataiRuntimeStatsTTFTStaleS     = "nycatai_runtime_stats_ttft_stale_seconds"

	nycataiEscapeGuardOff     = "off"
	nycataiEscapeGuardShadow  = "shadow"
	nycataiEscapeGuardEnforce = "enforce"

	nycataiSchedulerSettingsCacheTTL = 5 * time.Second
)

type nycataiSchedulerSettings struct {
	escapeGuard string
}

type cachedNycataiSchedulerSettings struct {
	nycataiSchedulerSettings
	expiresAt int64
}

var (
	nycataiSchedulerSettingsCache atomic.Value // *cachedNycataiSchedulerSettings
	nycataiSchedulerSettingsSF    singleflight.Group
	// 衰减参数放成包级原子量: 运行时统计的 report / snapshot 拿不到 service, 由设置加载时写入。
	nycataiErrorHalfLifeNanos atomic.Int64
	nycataiTTFTStaleNanos     atomic.Int64
	// 测试可替换。
	nycataiNow = time.Now
)

func resetNycataiSchedulerSettingsCacheForTest() {
	nycataiSchedulerSettingsCache = atomic.Value{}
	nycataiSchedulerSettingsSF = singleflight.Group{}
	nycataiErrorHalfLifeNanos.Store(0)
	nycataiTTFTStaleNanos.Store(0)
}

func parseNycataiEscapeGuard(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case nycataiEscapeGuardShadow:
		return nycataiEscapeGuardShadow
	case nycataiEscapeGuardEnforce:
		return nycataiEscapeGuardEnforce
	default:
		return nycataiEscapeGuardOff
	}
}

func parseNycataiSeconds(raw string) time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0
	}
	return time.Duration(value) * time.Second
}

// nycataiSchedulerSettings 读三个 NYCATAI 键(5 秒缓存), 顺带刷新衰减参数。读不到 = 全部关闭(与上游行为一致)。
func (s *OpenAIGatewayService) nycataiSchedulerSettings(ctx context.Context) nycataiSchedulerSettings {
	if cached, ok := nycataiSchedulerSettingsCache.Load().(*cachedNycataiSchedulerSettings); ok && cached != nil &&
		time.Now().UnixNano() < cached.expiresAt {
		return cached.nycataiSchedulerSettings
	}
	result, _, _ := nycataiSchedulerSettingsSF.Do("nycatai_scheduler_settings", func() (any, error) {
		values := map[string]string{}
		if repo := s.openAIAdvancedSchedulerSettingRepo(); repo != nil {
			dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openAIAdvancedSchedulerSettingDBTimeout)
			defer cancel()
			keys := []string{SettingKeyNycataiStickyEscapeGuard, SettingKeyNycataiRuntimeStatsErrorHalfLifeS, SettingKeyNycataiRuntimeStatsTTFTStaleS}
			if got, err := repo.GetMultiple(dbCtx, keys); err == nil {
				values = got
			} else {
				slog.Warn("nycatai_scheduler_settings_load_failed", "error", err)
			}
		}
		settings := nycataiSchedulerSettings{escapeGuard: parseNycataiEscapeGuard(values[SettingKeyNycataiStickyEscapeGuard])}
		nycataiErrorHalfLifeNanos.Store(int64(parseNycataiSeconds(values[SettingKeyNycataiRuntimeStatsErrorHalfLifeS])))
		nycataiTTFTStaleNanos.Store(int64(parseNycataiSeconds(values[SettingKeyNycataiRuntimeStatsTTFTStaleS])))
		nycataiSchedulerSettingsCache.Store(&cachedNycataiSchedulerSettings{
			nycataiSchedulerSettings: settings,
			expiresAt:                time.Now().Add(nycataiSchedulerSettingsCacheTTL).UnixNano(),
		})
		return settings, nil
	})
	settings, _ := result.(nycataiSchedulerSettings)
	if settings.escapeGuard == "" {
		settings.escapeGuard = nycataiEscapeGuardOff
	}
	return settings
}

// nycataiErrorDecayFactor: 距上次结果 elapsed 的错误率衰减系数(1 = 不衰减)。
func nycataiErrorDecayFactor(stat *openAIAccountRuntimeStat, now int64) float64 {
	halfLife := nycataiErrorHalfLifeNanos.Load()
	if halfLife <= 0 || stat == nil {
		return 1
	}
	last := stat.lastReportNanos.Load()
	if last <= 0 || now <= last {
		return 1
	}
	return math.Exp2(-float64(now-last) / float64(halfLife))
}

// nycataiTTFTStale: 首字样本是否已过期(过期视为无样本)。
func nycataiTTFTStale(stat *openAIAccountRuntimeStat, now int64) bool {
	stale := nycataiTTFTStaleNanos.Load()
	if stale <= 0 || stat == nil {
		return false
	}
	last := stat.lastTTFTNanos.Load()
	return last > 0 && now-last > stale
}

// nycataiStickyEscapeGuardAllows: 健康逃逸前调用。返回 false = 不逃逸(enforce 且组内没有未超线的候选号)。
func (s *defaultOpenAIAccountScheduler) nycataiStickyEscapeGuardAllows(
	ctx context.Context,
	req OpenAIAccountScheduleRequest,
	accountID int64,
	reason string,
	errorRate float64,
	ttft float64,
	cfg openAIStickyEscapeConfig,
) bool {
	if s == nil || s.service == nil {
		return true
	}
	mode := s.service.nycataiSchedulerSettings(ctx).escapeGuard
	if mode == nycataiEscapeGuardOff {
		return true
	}
	candidates, healthy, err := s.nycataiCountStickyEscapeTargets(ctx, req, accountID, cfg)
	if err != nil {
		// 查不到候选时按原逻辑逃逸(失败放行, 不比上游更差)。
		slog.Warn("sticky_escape_guard_check_failed", "account_id", accountID, "error", err)
		return true
	}
	if healthy > 0 {
		return true
	}
	var groupID int64
	if req.GroupID != nil {
		groupID = *req.GroupID
	}
	if mode == nycataiEscapeGuardShadow {
		slog.Info("sticky_escape_no_healthy_target",
			"account_id", accountID, "group_id", groupID, "reason", reason,
			"error_rate", errorRate, "ttft", ttft, "candidates", candidates, "mode", mode)
		return true
	}
	slog.Info("sticky_escape_suppressed_no_healthy_target",
		"account_id", accountID, "group_id", groupID, "reason", reason,
		"error_rate", errorRate, "ttft", ttft, "candidates", candidates, "mode", mode)
	return false
}

// nycataiCountStickyEscapeTargets 数逃逸后能去的号(与 selectByLoadBalance 同一套可用性过滤, 去掉原号和调用方排除的号),
// 以及其中未超线的个数。没有运行时统计的号算未超线(与评分里的中性处理一致)。
func (s *defaultOpenAIAccountScheduler) nycataiCountStickyEscapeTargets(
	ctx context.Context,
	req OpenAIAccountScheduleRequest,
	escapedAccountID int64,
	cfg openAIStickyEscapeConfig,
) (candidates int, healthy int, err error) {
	accounts, err := s.service.listSchedulableAccounts(ctx, req.GroupID, req.Platform)
	if err != nil {
		return 0, 0, err
	}
	accounts = s.filterGrokFreeQuotaAccounts(ctx, accounts)
	if req.Platform == PlatformGrok {
		now := time.Now()
		if filtered := filterGrokTeamModelRateLimitedAccounts(accounts, req.RequestedModel, now); filtered != nil {
			accounts = filtered
		}
		accounts = filterGrokModelQuotaBlockedAccounts(accounts, req.RequestedModel, now)
	}
	var schedGroup *Group
	if req.GroupID != nil && s.service.schedulerSnapshot != nil {
		schedGroup, _ = s.service.schedulerSnapshot.GetGroupByIDLite(ctx, *req.GroupID)
	}
	for i := range accounts {
		account := &accounts[i]
		if account.ID == escapedAccountID {
			continue
		}
		if req.ExcludedIDs != nil {
			if _, excluded := req.ExcludedIDs[account.ID]; excluded {
				continue
			}
		}
		if !account.IsSchedulable() || account.Platform != NormalizeOpenAICompatiblePlatform(req.Platform) || !account.IsOpenAICompatible() {
			continue
		}
		if s.service.isOpenAIAccountRequestRuntimeBlocked(account, req.RequestedModel) {
			continue
		}
		if schedGroup != nil && schedGroup.RequirePrivacySet && !account.IsPrivacySet() {
			continue
		}
		if !s.isAccountRequestCompatible(ctx, account, req) || !s.isAccountTransportCompatible(account, req.RequiredTransport) {
			continue
		}
		candidates++
		if _, _, _, over := s.shouldEscapeStickyAccount(account.ID, cfg); !over {
			healthy++
		}
	}
	return candidates, healthy, nil
}
