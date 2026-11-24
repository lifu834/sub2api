package service

// NYCATAI patch (conversation-reject-failover, 261124) 单测: 识别、开关、HTTP 400 换号不动号的状态、会话避号与回退。

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 取自 261124 生产 ops_error_logs 的原文。
const (
	nycataiLockBody          = `{"error":{"code":"conversation_safety_locked","message":"当前对话因上游安全拒绝已锁定","type":"invalid_request_error"}}`
	nycataiMdkjGenericBody   = `{"error":{"code":"invalid_request","message":"The request could not be processed. Please check the request parameters.","request_id":"c9ad2866","type":"invalid_request_error"},"request_id":"c9ad2866"}`
	nycataiUnknownParamBody  = `{"error":{"code":"unknown_parameter","message":"Unknown parameter: foo","type":"invalid_request_error"}}`
	nycataiSourcesdataReject = `{"error":{"code":"invalid_prompt","message":"上游安全检查拒绝了本次提示，已停止自动重试和换号重放。","type":"invalid_request_error"}}`
)

func nycataiRejectOn(t *testing.T) {
	t.Helper()
	nycataiConversationRejectFailoverOn.Store(true)
	t.Cleanup(func() {
		resetNycataiConversationAvoidForTest()
		resetNycataiLineagePinsForTest()
		resetNycataiSchedulerSettingsCacheForTest()
	})
}

func TestNycataiConversationReject_Kind(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   string
	}{
		{http.StatusBadRequest, nycataiLockBody, nycataiConversationRejectSafetyLock},
		{http.StatusBadRequest, `{"error":{"message":"当前对话因上游安全拒绝已锁定"}}`, nycataiConversationRejectSafetyLock},
		{http.StatusBadRequest, nycataiMdkjGenericBody, nycataiConversationRejectGenericInvalid},
		{http.StatusBadRequest, `{"error":{"code":"invalid_request","message":"The request could not be processed. Please check the request parameters"}}`, nycataiConversationRejectGenericInvalid},
		// 不认: 文案相同但 code 不同、别的 400、首次安全拒绝、非 400。
		{http.StatusBadRequest, `{"error":{"code":"bad_param","message":"The request could not be processed. Please check the request parameters."}}`, ""},
		{http.StatusBadRequest, nycataiUnknownParamBody, ""},
		{http.StatusBadRequest, nycataiSourcesdataReject, ""},
		{http.StatusBadGateway, nycataiLockBody, ""},
		{http.StatusForbidden, nycataiMdkjGenericBody, ""},
	}
	for _, tc := range cases {
		body := []byte(tc.body)
		got := nycataiConversationRejectKind(tc.status, extractUpstreamErrorCode(body), extractUpstreamErrorMessage(body), body)
		require.Equal(t, tc.want, got, "%d %s", tc.status, tc.body)
	}
}

func TestNycataiConversationReject_SettingLoad(t *testing.T) {
	groupID := int64(10961)
	for raw, want := range map[string]bool{"on": true, " ON ": true, "1": true, "off": false, "": false} {
		resetNycataiSchedulerSettingsCacheForTest()
		svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 24001, 24002, true), map[string]int64{},
			map[string]string{SettingKeyNycataiConversationRejectFailover: raw})
		svc.nycataiSchedulerSettings(context.Background())
		require.Equal(t, want, NycataiConversationRejectFailoverEnabled(), raw)
	}
}

type nycataiRejectForwardResult struct {
	err      error
	recorder *httptest.ResponseRecorder
	repo     *tempUnschedulableOpenAIAccountRepo
	ctx      *gin.Context
}

func nycataiForwardHTTPError(t *testing.T, status int, body string) nycataiRejectForwardResult {
	t.Helper()
	gin.SetMode(gin.TestMode)
	reqBody := []byte(`{"model":"gpt-6.2-sol","stream":true,"input":[{"type":"message","role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(reqBody))
	c.Request.Header.Set("Content-Type", "application/json")
	upstream := &httpUpstreamRecorder{responses: []*http.Response{{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"req_nycatai"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}}}
	repo := &tempUnschedulableOpenAIAccountRepo{}
	svc := &OpenAIGatewayService{
		cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{
			Enabled: false, AllowInsecureHTTP: true,
		}}},
		httpUpstream:     upstream,
		rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil),
	}
	account := &Account{
		ID: 24011, Name: "nycatai-reject", Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "http://upstream.example"},
	}
	_, err := svc.Forward(context.Background(), c, account, reqBody)
	return nycataiRejectForwardResult{err: err, recorder: rec, repo: repo, ctx: c}
}

// 打开: 站方对话锁 ⇒ 不写给客户, 返回换号错误; 不计号失败、不同号重试、不动号的状态。
func TestNycataiConversationReject_ForwardLockFailsOver(t *testing.T) {
	nycataiRejectOn(t)
	r := nycataiForwardHTTPError(t, http.StatusBadRequest, nycataiLockBody)

	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, r.err, &failoverErr)
	require.True(t, failoverErr.IsNycataiConversationReject())
	require.True(t, failoverErr.IsNycataiConversationScoped())
	require.False(t, failoverErr.IsNycataiEncryptedMismatch())
	require.True(t, failoverErr.ShouldRetryNextAccount())
	require.False(t, failoverErr.RetryableOnSameAccount)
	require.False(t, failoverErr.ShouldReportAccountScheduleFailure())
	require.Equal(t, http.StatusBadRequest, failoverErr.ClientStatusCode)
	require.Equal(t, "当前对话因上游安全拒绝已锁定", failoverErr.ClientMessage)
	require.Empty(t, r.recorder.Body.String(), "换号前不能写给客户")
	require.Zero(t, r.repo.modelRateLimitAccountID, "对话拒绝不能改号的状态")

	raw, ok := r.ctx.Get(OpsUpstreamErrorsKey)
	require.True(t, ok)
	events, ok := raw.([]*OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	require.Equal(t, "failover", events[0].Kind)
	require.Equal(t, int64(24011), events[0].AccountID)
	require.Equal(t, http.StatusBadRequest, events[0].UpstreamStatusCode)
}

// 打开: mdkj 通用 400 同样换号。
func TestNycataiConversationReject_ForwardMdkjGenericFailsOver(t *testing.T) {
	nycataiRejectOn(t)
	r := nycataiForwardHTTPError(t, http.StatusBadRequest, nycataiMdkjGenericBody)

	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, r.err, &failoverErr)
	require.True(t, failoverErr.IsNycataiConversationReject())
	require.Contains(t, failoverErr.ClientMessage, "could not be processed")
	require.Empty(t, r.recorder.Body.String())
}

// 打开: 其他 400 与补丁前一致(不换号)。
func TestNycataiConversationReject_ForwardOtherBadRequestUnchanged(t *testing.T) {
	nycataiRejectOn(t)
	for _, body := range []string{nycataiUnknownParamBody, nycataiSourcesdataReject} {
		r := nycataiForwardHTTPError(t, http.StatusBadRequest, body)
		var failoverErr *UpstreamFailoverError
		require.False(t, errors.As(r.err, &failoverErr) && failoverErr.IsNycataiConversationReject(), body)
	}
}

// 关闭(默认): 对话锁与补丁前一致, 不是换号错误。
func TestNycataiConversationReject_ForwardDefaultOffUnchanged(t *testing.T) {
	t.Cleanup(resetNycataiConversationAvoidForTest)
	nycataiConversationRejectFailoverOn.Store(false)
	r := nycataiForwardHTTPError(t, http.StatusBadRequest, nycataiLockBody)
	require.Error(t, r.err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(r.err, &failoverErr), "%v", r.err)
}

func TestNycataiConversationAvoid_Basics(t *testing.T) {
	t.Cleanup(resetNycataiConversationAvoidForTest)
	t.Cleanup(resetNycataiLineagePinsForTest)
	now := nycataiFixClock(t, time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC))
	svc := &OpenAIGatewayService{}
	g1, g2 := int64(1), int64(2)
	nycataiConversationRejectFailoverOn.Store(false)
	nycataiEncryptedMismatchFailoverOn.Store(false)

	// 两个开关都关: 不记、不生效。
	svc.NycataiAvoidAccountForConversation(&g1, "s1", 293)
	nycataiConversationRejectFailoverOn.Store(true)
	require.Nil(t, nycataiConversationAvoidedAccounts(&g1, "s1"))

	svc.NycataiAvoidAccountForConversation(&g1, "s1", 293)
	svc.NycataiAvoidAccountForConversation(&g1, "s1", 292)
	require.Equal(t, map[int64]struct{}{293: {}, 292: {}}, nycataiConversationAvoidedAccounts(&g1, "s1"))
	require.Nil(t, nycataiConversationAvoidedAccounts(&g2, "s1"), "按分组隔离")
	require.Nil(t, nycataiConversationAvoidedAccounts(&g1, ""), "空会话不记")

	// 只开加密不匹配开关也生效。
	nycataiConversationRejectFailoverOn.Store(false)
	nycataiEncryptedMismatchFailoverOn.Store(true)
	require.Len(t, nycataiConversationAvoidedAccounts(&g1, "s1"), 2)
	nycataiEncryptedMismatchFailoverOn.Store(false)
	require.Nil(t, nycataiConversationAvoidedAccounts(&g1, "s1"))
	nycataiConversationRejectFailoverOn.Store(true)

	merged, added := nycataiMergeConversationAvoid(&g1, "s1", map[int64]struct{}{187: {}})
	require.True(t, added)
	require.Equal(t, map[int64]struct{}{187: {}, 292: {}, 293: {}}, merged)
	_, added = nycataiMergeConversationAvoid(&g1, "s1", merged)
	require.False(t, added, "已全部排除时不再并入(避免递归)")

	*now = now.Add(nycataiConversationAvoidTTL + time.Second)
	require.Nil(t, nycataiConversationAvoidedAccounts(&g1, "s1"), "过期")
}

// 粘性绑在被避开的号上 ⇒ 选另一个号。
func TestNycataiConversationAvoid_SelectSkipsAvoidedStickyAccount(t *testing.T) {
	groupID := int64(10962)
	session := "nycatai_reject_avoid_sticky"
	key := "openai:" + session
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 24021, 24022, true),
		map[string]int64{key: 24021}, map[string]string{SettingKeyNycataiConversationRejectFailover: "on"})
	nycataiRejectOn(t)
	svc.NycataiAvoidAccountForConversation(&groupID, session, 24021)

	for i := 0; i < 5; i++ {
		selection, _ := nycataiSelect(t, svc, groupID, session, nil)
		require.Equal(t, int64(24022), selection.Account.ID)
	}
}

// 能用的号全被避开 ⇒ 退回原选法(不把客户的错误变成"无可用号")。
func TestNycataiConversationAvoid_FallsBackWhenNothingElse(t *testing.T) {
	groupID := int64(10963)
	session := "nycatai_reject_avoid_fallback"
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 24031, 24032, false),
		map[string]int64{}, map[string]string{SettingKeyNycataiConversationRejectFailover: "on"})
	nycataiRejectOn(t)
	svc.NycataiAvoidAccountForConversation(&groupID, session, 24031)

	selection, _ := nycataiSelect(t, svc, groupID, session, nil)
	require.Equal(t, int64(24031), selection.Account.ID)
}

// 开关关闭: 记录不生效, 粘性照常。
func TestNycataiConversationAvoid_IgnoredWhenSwitchOff(t *testing.T) {
	groupID := int64(10964)
	session := "nycatai_reject_avoid_off"
	key := "openai:" + session
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 24041, 24042, true),
		map[string]int64{key: 24041}, map[string]string{})
	nycataiRejectOn(t)
	svc.NycataiAvoidAccountForConversation(&groupID, session, 24041)
	nycataiConversationRejectFailoverOn.Store(false)

	selection, _ := nycataiSelect(t, svc, groupID, session, nil)
	require.Equal(t, int64(24041), selection.Account.ID)
}
