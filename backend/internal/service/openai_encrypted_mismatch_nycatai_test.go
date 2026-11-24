package service

// NYCATAI patch (encrypted-mismatch-failover, 261124) 单测: 识别、开关、流内写出前换号、不计号失败、会话钉号。

import (
	"context"
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

// 取自 261124 生产 ops_error_logs 的事件形状(id 已改短)。
const (
	nycataiMismatchFnOutputEvent = `{"type":"response.failed","response":{"access_programs":{"cyber":"standard"},"background":false,"error":{"code":"invalid_encrypted_content","message":"Encrypted function output content could not be decrypted or decoded.","param":null,"type":"invalid_request_error"},"id":"resp_fn","status":"failed"}}`
	nycataiMismatchMdkjEvent     = `{"request_id":"r1","response":{"created_at":1795504887,"error":{"code":"encrypted_context_invalid","message":"The upstream could not validate encrypted continuation context. Rebuild the conversation context in the client.","request_id":"r1","type":"invalid_request_error"},"id":"resp_mdkj","model":"gpt-6.2-sol","object":"response","status":"failed"},"sequence_number":1,"type":"response.failed"}`
	nycataiMismatchThinEvent     = `{"response":{"created_at":1794372477,"error":{"code":"invalid_request_error","message":"The encrypted content thin...4ifQ could not be verified. Reason: Encrypted content could not be decrypted or parsed.","type":"invalid_request_error"},"id":"resp_proxy_1","status":"failed"},"sequence_number":1,"type":"response.failed"}`
	nycataiMismatchRedactedEvent = `{"type":"response.failed","response":{"error":{"type":"invalid_request_error","message":"Upstream encrypted content error (details redacted)"},"status":"failed"}}`
)

func nycataiMismatchOn(t *testing.T) {
	t.Helper()
	nycataiEncryptedMismatchFailoverOn.Store(true)
	t.Cleanup(func() {
		resetNycataiLineagePinsForTest()
		resetNycataiSchedulerSettingsCacheForTest()
	})
}

func TestNycataiEncryptedMismatch_Detect(t *testing.T) {
	positives := []string{nycataiMismatchFnOutputEvent, nycataiMismatchMdkjEvent, nycataiMismatchThinEvent, nycataiMismatchRedactedEvent}
	for _, event := range positives {
		payload := []byte(event)
		require.True(t, isNycataiEncryptedContextMismatch(payload, extractOpenAISSEErrorMessage(payload)), event)
	}
	negatives := []string{
		`{"type":"response.failed","response":{"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: foo"}}}`,
		`{"type":"response.failed","response":{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"Your input exceeds the context window of this model."}}}`,
		`{"type":"response.failed","response":{"error":{"type":"invalid_request_error","code":"invalid_prompt","message":"Invalid prompt: your prompt was flagged as potentially violating our usage policy."}}}`,
		`{"type":"response.failed","response":{"error":{"type":"server_error","message":"The server had an error while processing your request."}}}`,
	}
	for _, event := range negatives {
		payload := []byte(event)
		require.False(t, isNycataiEncryptedContextMismatch(payload, extractOpenAISSEErrorMessage(payload)), event)
	}
}

// 开关关闭(默认): 判定与补丁前一致(invalid_request 不换号); 打开: 换号。
func TestNycataiEncryptedMismatch_ShouldFailoverFollowsSwitch(t *testing.T) {
	t.Cleanup(resetNycataiLineagePinsForTest)
	for _, event := range []string{nycataiMismatchFnOutputEvent, nycataiMismatchMdkjEvent, nycataiMismatchThinEvent} {
		payload := []byte(event)
		message := extractOpenAISSEErrorMessage(payload)
		nycataiEncryptedMismatchFailoverOn.Store(false)
		require.False(t, openAIStreamFailedEventShouldFailover(payload, message), event)
		nycataiEncryptedMismatchFailoverOn.Store(true)
		require.True(t, openAIStreamFailedEventShouldFailover(payload, message), event)
		require.True(t, openAIStreamErrorEventShouldFailover(payload, message), event)
	}
	// 打开时其他 invalid_request 仍不换号。
	other := []byte(`{"type":"response.failed","response":{"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: foo"}}}`)
	require.False(t, openAIStreamFailedEventShouldFailover(other, extractOpenAISSEErrorMessage(other)))
}

func TestNycataiEncryptedMismatch_SettingLoad(t *testing.T) {
	groupID := int64(10951)
	for raw, want := range map[string]bool{"on": true, " ON ": true, "true": true, "off": false, "": false, "shadow": false} {
		resetNycataiSchedulerSettingsCacheForTest()
		svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 23001, 23002, true), map[string]int64{},
			map[string]string{SettingKeyNycataiEncryptedMismatchFailover: raw})
		svc.nycataiSchedulerSettings(context.Background())
		require.Equal(t, want, NycataiEncryptedMismatchFailoverEnabled(), raw)
	}
}

func nycataiDriveStream(t *testing.T, upstream string) (*openaiStreamingResult, error, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	repo := &openAIStream403AccountRepo{}
	svc := &OpenAIGatewayService{
		cfg:              &config.Config{},
		toolCorrector:    NewCodexToolCorrector(),
		rateLimitService: &RateLimitService{accountRepo: repo},
	}
	account := &Account{ID: 23011, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true}
	recorder := newOpenAIResponseFlushRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(upstream)),
	}
	result, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "gpt-6.2-sol", "gpt-6.2-sol")
	require.Zero(t, repo.setErrorCalls)
	recorder.mu.Lock()
	body := recorder.body.String()
	recorder.mu.Unlock()
	return result, err, body
}

// 打开: 首个事件就是加密不匹配 ⇒ 不写给客户, 返回可换号、不计号失败的错误。
func TestNycataiEncryptedMismatch_StreamFailsOverBeforeWrite(t *testing.T) {
	nycataiMismatchOn(t)
	upstream := "event: response.failed\ndata: " + nycataiMismatchMdkjEvent + "\n\n"
	_, err, body := nycataiDriveStream(t, upstream)

	failoverErr, ok := err.(*UpstreamFailoverError)
	require.True(t, ok, "%v", err)
	require.True(t, failoverErr.IsNycataiEncryptedMismatch())
	require.True(t, failoverErr.ShouldRetryNextAccount())
	require.False(t, failoverErr.ShouldReportAccountScheduleFailure())
	require.False(t, failoverErr.RetryableOnSameAccount)
	require.Equal(t, http.StatusBadRequest, failoverErr.ClientStatusCode)
	require.Contains(t, failoverErr.ClientMessage, "continuation context")
	require.NotContains(t, body, "continuation context")
}

// 关闭(默认): 与补丁前一致, failed 事件原样写给客户, 不是换号错误。
func TestNycataiEncryptedMismatch_StreamDefaultOffPassesThrough(t *testing.T) {
	t.Cleanup(resetNycataiLineagePinsForTest)
	nycataiEncryptedMismatchFailoverOn.Store(false)
	upstream := "event: response.failed\ndata: " + nycataiMismatchFnOutputEvent + "\n\n"
	_, err, body := nycataiDriveStream(t, upstream)

	require.Error(t, err)
	_, isFailover := err.(*UpstreamFailoverError)
	require.False(t, isFailover)
	require.Contains(t, body, "could not be decrypted")
}

// 已向客户写出语义内容后不换号(重放不安全), 打开时也一样。
func TestNycataiEncryptedMismatch_NoFailoverAfterOutput(t *testing.T) {
	nycataiMismatchOn(t)
	upstream := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n" +
		"event: response.failed\ndata: " + nycataiMismatchFnOutputEvent + "\n\n"
	_, err, body := nycataiDriveStream(t, upstream)

	require.Error(t, err)
	_, isFailover := err.(*UpstreamFailoverError)
	require.False(t, isFailover)
	require.Contains(t, body, "partial")
}

func TestNycataiEncryptedMismatch_SwitchCap(t *testing.T) {
	require.False(t, NycataiEncryptedMismatchSwitchesExhausted(0))
	require.False(t, NycataiEncryptedMismatchSwitchesExhausted(nycataiEncryptedMismatchMaxSwitches-1))
	require.True(t, NycataiEncryptedMismatchSwitchesExhausted(nycataiEncryptedMismatchMaxSwitches))
}

func TestNycataiEncryptedLineagePin_Basics(t *testing.T) {
	t.Cleanup(resetNycataiLineagePinsForTest)
	now := nycataiFixClock(t, time.Date(2026, 11, 24, 8, 0, 0, 0, time.UTC))
	svc := &OpenAIGatewayService{}
	g1, g2 := int64(1), int64(2)

	// 关闭时不记。
	nycataiEncryptedMismatchFailoverOn.Store(false)
	svc.NycataiPinEncryptedLineage(&g1, "s1", 291)
	nycataiEncryptedMismatchFailoverOn.Store(true)
	require.Zero(t, nycataiEncryptedLineagePinnedAccount(&g1, "s1"))

	svc.NycataiPinEncryptedLineage(&g1, "s1", 291)
	require.Equal(t, int64(291), nycataiEncryptedLineagePinnedAccount(&g1, "s1"))
	require.True(t, nycataiEncryptedLineagePinned(&g1, "s1", 291))
	require.False(t, nycataiEncryptedLineagePinned(&g1, "s1", 187))
	require.Zero(t, nycataiEncryptedLineagePinnedAccount(&g2, "s1"), "按分组隔离")
	require.Zero(t, nycataiEncryptedLineagePinnedAccount(&g1, ""), "空会话不钉")

	// 关闭后不生效(记录还在, 重新打开恢复)。
	nycataiEncryptedMismatchFailoverOn.Store(false)
	require.Zero(t, nycataiEncryptedLineagePinnedAccount(&g1, "s1"))
	nycataiEncryptedMismatchFailoverOn.Store(true)

	// 过期。
	*now = now.Add(nycataiLineagePinTTL + time.Second)
	require.Zero(t, nycataiEncryptedLineagePinnedAccount(&g1, "s1"))
}

// 会话钉在慢号上: 不做健康逃逸, 绑定不变。
func TestNycataiEncryptedLineagePin_SuppressesStickyEscape(t *testing.T) {
	groupID := int64(10952)
	session := "nycatai_mismatch_pin_escape"
	key := "openai:" + session
	svc, cache := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 23021, 23022, true),
		map[string]int64{key: 23021}, map[string]string{SettingKeyNycataiEncryptedMismatchFailover: "on"})
	nycataiMismatchOn(t)
	nycataiReportSlow(svc, 23021)
	nycataiReportFast(svc, 23022)
	svc.NycataiPinEncryptedLineage(&groupID, session, 23021)

	for i := 0; i < 5; i++ {
		selection, decision := nycataiSelect(t, svc, groupID, session, nil)
		require.Equal(t, int64(23021), selection.Account.ID)
		require.Equal(t, openAIAccountScheduleLayerSessionSticky, decision.Layer)
		require.Equal(t, int64(23021), cache.sessionBindings[key])
	}
}

// 对照: 同样条件不钉号 ⇒ 照常逃逸(补丁不改变未钉会话的行为)。
func TestNycataiEncryptedLineagePin_UnpinnedStillEscapes(t *testing.T) {
	groupID := int64(10953)
	session := "nycatai_mismatch_unpinned"
	key := "openai:" + session
	svc, cache := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 23031, 23032, true),
		map[string]int64{key: 23031}, map[string]string{SettingKeyNycataiEncryptedMismatchFailover: "on"})
	nycataiMismatchOn(t)
	nycataiReportSlow(svc, 23031)
	nycataiReportFast(svc, 23032)

	selection, _ := nycataiSelect(t, svc, groupID, session, nil)
	require.Equal(t, int64(23032), selection.Account.ID)
	require.Equal(t, int64(23032), cache.sessionBindings[key])
}

// 粘性已过期(无绑定)但钉过号 ⇒ 回到钉住的号, 即使它更慢。
func TestNycataiEncryptedLineagePin_RestoresExpiredBinding(t *testing.T) {
	groupID := int64(10954)
	session := "nycatai_mismatch_pin_restore"
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 23041, 23042, true),
		map[string]int64{}, map[string]string{SettingKeyNycataiEncryptedMismatchFailover: "on"})
	nycataiMismatchOn(t)
	nycataiReportFast(svc, 23041)
	nycataiReportSlow(svc, 23042)
	svc.NycataiPinEncryptedLineage(&groupID, session, 23042)

	for i := 0; i < 5; i++ {
		selection, _ := nycataiSelect(t, svc, groupID, session, nil)
		require.Equal(t, int64(23042), selection.Account.ID)
	}
}

// 钉住的号不可调度 ⇒ 照常换走。
func TestNycataiEncryptedLineagePin_IgnoredWhenAccountUnschedulable(t *testing.T) {
	groupID := int64(10955)
	session := "nycatai_mismatch_pin_unschedulable"
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 23051, 23052, false),
		map[string]int64{}, map[string]string{SettingKeyNycataiEncryptedMismatchFailover: "on"})
	nycataiMismatchOn(t)
	svc.NycataiPinEncryptedLineage(&groupID, session, 23052)

	selection, _ := nycataiSelect(t, svc, groupID, session, nil)
	require.Equal(t, int64(23051), selection.Account.ID)
}

// 钉住的号被本请求排除(刚在它上面失败) ⇒ 不选它。
func TestNycataiEncryptedLineagePin_RespectsExcluded(t *testing.T) {
	groupID := int64(10956)
	session := "nycatai_mismatch_pin_excluded"
	svc, _ := nycataiGuardTestService(t, groupID, nycataiTwoAccounts(groupID, 23061, 23062, true),
		map[string]int64{}, map[string]string{SettingKeyNycataiEncryptedMismatchFailover: "on"})
	nycataiMismatchOn(t)
	svc.NycataiPinEncryptedLineage(&groupID, session, 23062)

	selection, _ := nycataiSelect(t, svc, groupID, session, map[int64]struct{}{23062: {}})
	require.Equal(t, int64(23061), selection.Account.ID)
}
