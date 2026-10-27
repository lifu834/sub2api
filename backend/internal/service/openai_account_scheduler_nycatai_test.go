package service

// NYCATAI patch (sticky-escape-guard, 261027) 单测: 逃逸守门 off / shadow / enforce, 运行时统计时间衰减。

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func nycataiGuardTestService(t *testing.T, groupID int64, accounts []Account, binding map[string]int64, settings map[string]string) (*OpenAIGatewayService, *schedulerTestGatewayCache) {
	t.Helper()
	cache := &schedulerTestGatewayCache{sessionBindings: binding}
	cfg := &config.Config{}
	cfg.Gateway.OpenAIScheduler.StickyEscapeEnabled = true
	cfg.Gateway.OpenAIScheduler.StickyEscapeTTFTMs = 15000
	cfg.Gateway.OpenAIScheduler.StickyEscapeErrorRate = 0.5
	rl := newOpenAIAdvancedSchedulerRateLimitService("true")
	repo := rl.settingService.settingRepo.(*openAIAdvancedSchedulerSettingRepoStub)
	for k, v := range settings {
		repo.values[k] = v
	}
	t.Cleanup(resetNycataiSchedulerSettingsCacheForTest)
	return &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cache:              cache,
		cfg:                cfg,
		rateLimitService:   rl,
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
		openaiAccountStats: newOpenAIAccountRuntimeStats(),
	}, cache
}

func nycataiReportSlow(svc *OpenAIGatewayService, ids ...int64) {
	slow := 60000
	for _, id := range ids {
		for i := 0; i < 3; i++ {
			svc.openaiAccountStats.report(id, true, &slow)
		}
	}
}

func nycataiReportFast(svc *OpenAIGatewayService, ids ...int64) {
	fast := 3000
	for _, id := range ids {
		for i := 0; i < 3; i++ {
			svc.openaiAccountStats.report(id, true, &fast)
		}
	}
}

func nycataiTwoAccounts(groupID int64, a, b int64, bSchedulable bool) []Account {
	return []Account{
		{ID: a, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 10, Priority: 0, GroupIDs: []int64{groupID}},
		{ID: b, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: bSchedulable, Concurrency: 10, Priority: 0, GroupIDs: []int64{groupID}},
	}
}

func nycataiSelect(t *testing.T, svc *OpenAIGatewayService, groupID int64, session string, excluded map[int64]struct{}) (*AccountSelectionResult, OpenAIAccountScheduleDecision) {
	t.Helper()
	selection, decision, err := svc.SelectAccountWithScheduler(context.Background(), &groupID, "", session, "gpt-5.1", excluded, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.NotNil(t, selection.Account)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
	return selection, decision
}

// enforce: 两个号都超线 ⇒ 不逃逸, 会话留在原号, 绑定不变。
func TestNycataiStickyEscapeGuard_EnforceKeepsStickyWhenNoHealthyTarget(t *testing.T) {
	groupID := int64(10901)
	key := "openai:nycatai_guard_enforce_all_slow"
	svc, cache := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 22901, 22902, true),
		map[string]int64{key: 22901}, map[string]string{SettingKeyNycataiStickyEscapeGuard: "enforce"})
	nycataiReportSlow(svc, 22901, 22902)

	for i := 0; i < 5; i++ {
		selection, decision := nycataiSelect(t, svc, groupID, "nycatai_guard_enforce_all_slow", nil)
		require.Equal(t, int64(22901), selection.Account.ID)
		require.Equal(t, openAIAccountScheduleLayerSessionSticky, decision.Layer)
		require.True(t, decision.StickySessionHit)
		require.Equal(t, int64(22901), cache.sessionBindings[key])
	}
}

// enforce: 另一个号健康 ⇒ 照常逃逸并改绑到它。
func TestNycataiStickyEscapeGuard_EnforceEscapesWhenHealthyTargetExists(t *testing.T) {
	groupID := int64(10902)
	key := "openai:nycatai_guard_enforce_healthy"
	svc, cache := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 22911, 22912, true),
		map[string]int64{key: 22911}, map[string]string{SettingKeyNycataiStickyEscapeGuard: "enforce"})
	nycataiReportSlow(svc, 22911)
	nycataiReportFast(svc, 22912)

	selection, decision := nycataiSelect(t, svc, groupID, "nycatai_guard_enforce_healthy", nil)
	require.Equal(t, int64(22912), selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	require.Equal(t, int64(22912), cache.sessionBindings[key])
}

// enforce: 没有运行时统计的号算健康(与评分中性处理一致) ⇒ 逃逸。
func TestNycataiStickyEscapeGuard_EnforceTreatsUnseenAccountAsHealthy(t *testing.T) {
	groupID := int64(10903)
	key := "openai:nycatai_guard_enforce_unseen"
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 22921, 22922, true),
		map[string]int64{key: 22921}, map[string]string{SettingKeyNycataiStickyEscapeGuard: "enforce"})
	nycataiReportSlow(svc, 22921)

	selection, _ := nycataiSelect(t, svc, groupID, "nycatai_guard_enforce_unseen", nil)
	require.Equal(t, int64(22922), selection.Account.ID)
}

// shadow: 都超线也照常逃逸(只打日志)。
func TestNycataiStickyEscapeGuard_ShadowStillEscapes(t *testing.T) {
	groupID := int64(10904)
	key := "openai:nycatai_guard_shadow"
	svc, cache := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 22931, 22932, true),
		map[string]int64{key: 22931}, map[string]string{SettingKeyNycataiStickyEscapeGuard: "shadow"})
	nycataiReportSlow(svc, 22931, 22932)

	selection, decision := nycataiSelect(t, svc, groupID, "nycatai_guard_shadow", nil)
	require.Equal(t, int64(22932), selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	require.Equal(t, int64(22932), cache.sessionBindings[key])
}

// 默认(无设置)= off: 行为与补丁前一致。
func TestNycataiStickyEscapeGuard_DefaultOffUnchanged(t *testing.T) {
	groupID := int64(10905)
	key := "openai:nycatai_guard_default_off"
	svc, cache := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 22941, 22942, true),
		map[string]int64{key: 22941}, nil)
	nycataiReportSlow(svc, 22941, 22942)

	selection, _ := nycataiSelect(t, svc, groupID, "nycatai_guard_default_off", nil)
	require.Equal(t, int64(22942), selection.Account.ID)
	require.Equal(t, int64(22942), cache.sessionBindings[key])
}

// enforce: 另一个号不可调度 / 被调用方排除 ⇒ 不算候选 ⇒ 留在原号。
func TestNycataiStickyEscapeGuard_EnforceIgnoresUnusableTargets(t *testing.T) {
	groupID := int64(10906)
	key := "openai:nycatai_guard_unusable"
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 22951, 22952, false),
		map[string]int64{key: 22951}, map[string]string{SettingKeyNycataiStickyEscapeGuard: "enforce"})
	nycataiReportSlow(svc, 22951)
	selection, decision := nycataiSelect(t, svc, groupID, "nycatai_guard_unusable", nil)
	require.Equal(t, int64(22951), selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerSessionSticky, decision.Layer)

	groupID2 := int64(10907)
	key2 := "openai:nycatai_guard_excluded"
	svc2, _ := nycataiGuardTestService(t, groupID2, nycataiTwoAccounts(groupID2, 22961, 22962, true),
		map[string]int64{key2: 22961}, map[string]string{SettingKeyNycataiStickyEscapeGuard: "enforce"})
	nycataiReportSlow(svc2, 22961)
	nycataiReportFast(svc2, 22962)
	selection, decision = nycataiSelect(t, svc2, groupID2, "nycatai_guard_excluded", map[int64]struct{}{22962: {}})
	require.Equal(t, int64(22961), selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerSessionSticky, decision.Layer)
}

func TestNycataiSchedulerSettings_Parse(t *testing.T) {
	require.Equal(t, nycataiEscapeGuardEnforce, parseNycataiEscapeGuard(" Enforce "))
	require.Equal(t, nycataiEscapeGuardShadow, parseNycataiEscapeGuard("shadow"))
	require.Equal(t, nycataiEscapeGuardOff, parseNycataiEscapeGuard("yes"))
	require.Equal(t, nycataiEscapeGuardOff, parseNycataiEscapeGuard(""))
	require.Equal(t, 600*time.Second, parseNycataiSeconds("600"))
	require.Equal(t, time.Duration(0), parseNycataiSeconds("-5"))
	require.Equal(t, time.Duration(0), parseNycataiSeconds("x"))

	// 设置表里的衰减参数随加载写入原子量。
	groupID := int64(10908)
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 22971, 22972, true), map[string]int64{},
		map[string]string{SettingKeyNycataiRuntimeStatsErrorHalfLifeS: "600", SettingKeyNycataiRuntimeStatsTTFTStaleS: "900"})
	require.Equal(t, nycataiEscapeGuardOff, svc.nycataiSchedulerSettings(context.Background()).escapeGuard)
	require.Equal(t, int64(600*time.Second), nycataiErrorHalfLifeNanos.Load())
	require.Equal(t, int64(900*time.Second), nycataiTTFTStaleNanos.Load())
}

func nycataiFixClock(t *testing.T, start time.Time) *time.Time {
	t.Helper()
	now := start
	orig := nycataiNow
	nycataiNow = func() time.Time { return now }
	t.Cleanup(func() {
		nycataiNow = orig
		resetNycataiSchedulerSettingsCacheForTest()
	})
	return &now
}

// 错误率按半衰期衰减; 衰减后再来一个成功样本, 是在衰减后的值上合并。
func TestNycataiRuntimeStats_ErrorRateDecay(t *testing.T) {
	now := nycataiFixClock(t, time.Unix(1_800_000_000, 0))
	nycataiErrorHalfLifeNanos.Store(int64(60 * time.Second))
	stats := newOpenAIAccountRuntimeStats()
	for i := 0; i < 5; i++ {
		stats.report(23001, false, nil)
	}
	errorRate, _, _ := stats.snapshot(23001)
	require.InDelta(t, 0.67232, errorRate, 1e-4)

	*now = now.Add(120 * time.Second) // 两个半衰期
	errorRate, _, _ = stats.snapshot(23001)
	require.InDelta(t, 0.67232/4, errorRate, 1e-4)

	stats.report(23001, true, nil)
	errorRate, _, _ = stats.snapshot(23001)
	require.InDelta(t, 0.8*0.67232/4, errorRate, 1e-4)
}

// 首字超过过期时长视为无样本; 之后的新样本直接作初值。
func TestNycataiRuntimeStats_TTFTStale(t *testing.T) {
	now := nycataiFixClock(t, time.Unix(1_800_000_000, 0))
	nycataiTTFTStaleNanos.Store(int64(600 * time.Second))
	stats := newOpenAIAccountRuntimeStats()
	slow := 60000
	stats.report(23011, true, &slow)
	_, ttft, has := stats.snapshot(23011)
	require.True(t, has)
	require.Equal(t, 60000.0, ttft)

	*now = now.Add(700 * time.Second)
	_, _, has = stats.snapshot(23011)
	require.False(t, has)

	fast := 3000
	stats.report(23011, true, &fast)
	_, ttft, has = stats.snapshot(23011)
	require.True(t, has)
	require.Equal(t, 3000.0, ttft)
}

// 衰减关闭(默认)时, 时间流逝不改变统计值。
func TestNycataiRuntimeStats_DecayOffUnchanged(t *testing.T) {
	now := nycataiFixClock(t, time.Unix(1_800_000_000, 0))
	stats := newOpenAIAccountRuntimeStats()
	slow := 60000
	for i := 0; i < 5; i++ {
		stats.report(23021, false, &slow)
	}
	e1, t1, h1 := stats.snapshot(23021)
	*now = now.Add(24 * time.Hour)
	e2, t2, h2 := stats.snapshot(23021)
	require.Equal(t, e1, e2)
	require.Equal(t, t1, t2)
	require.Equal(t, h1, h2)
}
