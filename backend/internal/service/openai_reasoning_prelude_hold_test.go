package service

import (
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

// NYCATAI 推理暂存补丁的用例。事件序列取自 261022 生产实测：
// 过载失败 = created → in_progress → metadata → failed；
// 高推理成功 = created → in_progress → reasoning(带 encrypted_content) → 摘要 → 正文 → metadata → completed。

const reasoningHoldOverloadedFailed = `data: {"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"Our servers are currently overloaded. Please try again later.","type":"service_unavailable_error"}},"sequence_number":9}`

func reasoningHoldEvent(eventType, data string) string {
	return "event: " + eventType + "\ndata: " + data + "\n\n"
}

var (
	reasoningHoldCreated    = reasoningHoldEvent("response.created", `{"type":"response.created","response":{"id":"resp_1","status":"in_progress"},"sequence_number":0}`)
	reasoningHoldInProgress = reasoningHoldEvent("response.in_progress", `{"type":"response.in_progress","response":{"id":"resp_1","status":"in_progress"},"sequence_number":1}`)
	reasoningHoldMetadata   = reasoningHoldEvent("response.metadata", `{"type":"response.metadata","response_id":"resp_1","metadata":{"openai_chatgpt_moderation_metadata":{}},"sequence_number":2}`)
	reasoningHoldRsAdded    = reasoningHoldEvent("response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","encrypted_content":"gAAAAB-enc","summary":[]},"sequence_number":2}`)
	reasoningHoldSumPart    = reasoningHoldEvent("response.reasoning_summary_part.added", `{"type":"response.reasoning_summary_part.added","item_id":"rs_1","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":""},"sequence_number":3}`)
	reasoningHoldSumDelta   = reasoningHoldEvent("response.reasoning_summary_text.delta", `{"type":"response.reasoning_summary_text.delta","item_id":"rs_1","output_index":0,"summary_index":0,"delta":"**Planning the answer**","sequence_number":4}`)
	reasoningHoldSumDone    = reasoningHoldEvent("response.reasoning_summary_text.done", `{"type":"response.reasoning_summary_text.done","item_id":"rs_1","output_index":0,"summary_index":0,"text":"**Planning the answer**","sequence_number":5}`)
	reasoningHoldRsDone     = reasoningHoldEvent("response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","encrypted_content":"gAAAAB-enc","summary":[{"type":"summary_text","text":"**Planning the answer**"}]},"sequence_number":6}`)
	reasoningHoldMsgAdded   = reasoningHoldEvent("response.output_item.added", `{"type":"response.output_item.added","output_index":1,"item":{"id":"msg_1","type":"message","role":"assistant","content":[]},"sequence_number":7}`)
	reasoningHoldPartAdded  = reasoningHoldEvent("response.content_part.added", `{"type":"response.content_part.added","item_id":"msg_1","output_index":1,"content_index":0,"part":{"type":"output_text","text":""},"sequence_number":8}`)
	reasoningHoldTextDelta  = reasoningHoldEvent("response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_1","output_index":1,"content_index":0,"delta":"ok","sequence_number":9}`)
	reasoningHoldCompleted  = reasoningHoldEvent("response.completed", `{"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[{"id":"rs_1","type":"reasoning","summary":[]},{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}},"sequence_number":10}`)
	reasoningHoldFailed     = "event: response.failed\n" + reasoningHoldOverloadedFailed + "\n\n"
	reasoningHoldReasoning  = reasoningHoldRsAdded + reasoningHoldSumPart + reasoningHoldSumDelta + reasoningHoldSumDone + reasoningHoldRsDone
)

type reasoningHoldRun struct {
	body string
	err  error
	c    *gin.Context
}

// runReasoningHoldStream 把 head 立即写给处理器，等 delay 后再写 tail。
func runReasoningHoldStream(t *testing.T, keepaliveSeconds int, head string, delay time.Duration, tail string) reasoningHoldRun {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize, StreamKeepaliveInterval: keepaliveSeconds}}
	svc := &OpenAIGatewayService{cfg: cfg}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte(head))
		time.Sleep(delay)
		_, _ = pw.Write([]byte(tail))
		_ = pw.Close()
	}()
	resp := &http.Response{StatusCode: http.StatusOK, Body: pr, Header: http.Header{"X-Request-Id": []string{"rid-reasoning-hold"}}}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Name: "acc"}
	_, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "model", "model")
	return reasoningHoldRun{body: rec.Body.String(), err: err, c: c}
}

func reasoningHoldEventTypes(stream string) []string {
	var types []string
	for _, line := range strings.Split(stream, "\n") {
		if eventType, ok := extractOpenAISSEEventLine(line); ok {
			types = append(types, strings.TrimSpace(eventType))
		}
	}
	return types
}

func requireReasoningHoldFailover(t *testing.T, run reasoningHoldRun) {
	t.Helper()
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, run.err, &failoverErr, "body=%q", run.body)
	// 只允许出现 keepalive 注释，不能有任何 SSE 数据。
	require.NotContains(t, run.body, "data:")
	require.NotContains(t, run.body, "rs_1")
	require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(run.c))
}

func TestOpenAIStreamMetadataBeforeFailureFailsOver(t *testing.T) {
	run := runReasoningHoldStream(t, 0, reasoningHoldCreated+reasoningHoldInProgress+reasoningHoldMetadata, 0, reasoningHoldFailed)
	requireReasoningHoldFailover(t, run)
	require.Empty(t, run.body)
}

func TestOpenAIStreamReasoningPreludeFailureFailsOver(t *testing.T) {
	run := runReasoningHoldStream(t, 0, reasoningHoldCreated+reasoningHoldInProgress+reasoningHoldReasoning+reasoningHoldMetadata, 0, reasoningHoldFailed)
	requireReasoningHoldFailover(t, run)
	require.Empty(t, run.body)
}

func TestOpenAIStreamReasoningPreludeFailureWithKeepaliveFailsOver(t *testing.T) {
	run := runReasoningHoldStream(t, 1, reasoningHoldCreated+reasoningHoldInProgress+reasoningHoldReasoning, 2500*time.Millisecond, reasoningHoldFailed)
	requireReasoningHoldFailover(t, run)
	// 暂存期间下游收到 keepalive，连接不会被中间层判空闲。
	require.Contains(t, run.body, ":\n\n")
	require.Equal(t, "", strings.ReplaceAll(run.body, ":\n\n", ""))
}

func TestOpenAIStreamReasoningPreludeReleasedWithText(t *testing.T) {
	stream := reasoningHoldCreated + reasoningHoldInProgress + reasoningHoldReasoning +
		reasoningHoldMsgAdded + reasoningHoldPartAdded + reasoningHoldTextDelta + reasoningHoldMetadata + reasoningHoldCompleted
	run := runReasoningHoldStream(t, 0, stream, 0, "")
	require.NoError(t, run.err)
	// 暂存的推理事件随正文一起按原顺序发出，一个不少。
	require.Equal(t, reasoningHoldEventTypes(stream), reasoningHoldEventTypes(run.body))
	require.Contains(t, run.body, "Planning the answer")
	require.Less(t, strings.Index(run.body, "rs_1"), strings.Index(run.body, `"delta":"ok"`))
}

func TestOpenAIStreamReasoningPreludeReleasedWithKeepaliveGap(t *testing.T) {
	head := reasoningHoldCreated + reasoningHoldInProgress + reasoningHoldReasoning
	tail := reasoningHoldMsgAdded + reasoningHoldPartAdded + reasoningHoldTextDelta + reasoningHoldCompleted
	run := runReasoningHoldStream(t, 1, head, 2500*time.Millisecond, tail)
	require.NoError(t, run.err)
	require.Equal(t, reasoningHoldEventTypes(head+tail), reasoningHoldEventTypes(run.body))
	// keepalive 只出现在推理事件之前（推理被暂存，没有提前写出）。
	require.Less(t, strings.LastIndex(run.body, ":\n\n"), strings.Index(run.body, "rs_1"))
}

func TestOpenAIStreamReasoningOnlyCompletedIsDelivered(t *testing.T) {
	stream := reasoningHoldCreated + reasoningHoldInProgress + reasoningHoldReasoning + reasoningHoldCompleted
	run := runReasoningHoldStream(t, 0, stream, 0, "")
	require.NoError(t, run.err)
	require.Equal(t, reasoningHoldEventTypes(stream), reasoningHoldEventTypes(run.body))
}

func TestOpenAIStreamReasoningPreludeNonRetryableFailureDeliversHeldEvents(t *testing.T) {
	failed := "event: response.failed\n" +
		`data: {"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"code":"invalid_prompt","message":"Invalid prompt","type":"invalid_request_error"}},"sequence_number":9}` + "\n\n"
	run := runReasoningHoldStream(t, 0, reasoningHoldCreated+reasoningHoldInProgress+reasoningHoldReasoning, 0, failed)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(run.err, &failoverErr))
	require.Error(t, run.err)
	// 不可重试的失败与原逻辑一致：推理事件和失败终态一起交给客户端。
	require.Contains(t, run.body, "rs_1")
	require.Contains(t, run.body, "Planning the answer")
	require.Equal(t, 1, strings.Count(run.body, "event: response.failed"))
	require.Less(t, strings.Index(run.body, "rs_1"), strings.Index(run.body, "response.failed"))
}

func TestOpenAIStreamFailureAfterTextStillSuppressesFailover(t *testing.T) {
	head := reasoningHoldCreated + reasoningHoldInProgress + reasoningHoldReasoning +
		reasoningHoldMsgAdded + reasoningHoldPartAdded + reasoningHoldTextDelta
	run := runReasoningHoldStream(t, 0, head, 0, reasoningHoldFailed)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(run.err, &failoverErr))
	require.Contains(t, run.body, `"delta":"ok"`)
	require.Contains(t, run.body, "rs_1")
	require.Contains(t, run.body, "response.failed")
}

func TestOpenAIStreamReasoningPreludeHoldStopsAtCap(t *testing.T) {
	big := strings.Repeat("A", openAIReasoningPreludeHoldMaxBytes+1024)
	bigDone := reasoningHoldEvent("response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","encrypted_content":"`+big+`","summary":[]},"sequence_number":6}`)
	head := reasoningHoldCreated + reasoningHoldInProgress + reasoningHoldRsAdded + bigDone + reasoningHoldSumDelta
	run := runReasoningHoldStream(t, 0, head, 0, reasoningHoldFailed)
	// 暂存超过上限后按原逻辑提交，不会变成暂存超限 failover；之后的失败照旧不换号。
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(run.err, &failoverErr), "err=%v", run.err)
	require.Contains(t, run.body, big)
	require.Contains(t, run.body, "response.failed")
}

func TestOpenAIStreamEventIsReasoningPrelude(t *testing.T) {
	cases := []struct {
		eventType string
		data      string
		want      bool
	}{
		{"response.output_item.added", `{"item":{"type":"reasoning","encrypted_content":"x"}}`, true},
		{"response.output_item.done", `{"item":{"type":"reasoning"}}`, true},
		{"response.output_item.added", `{"item":{"type":"message","content":[]}}`, false},
		{"response.output_item.added", `{"item":{"type":"function_call","arguments":""}}`, false},
		{"response.reasoning_summary_part.added", `{}`, true},
		{"response.reasoning_summary_part.done", `{}`, true},
		{"response.reasoning_summary_text.delta", `{"delta":"x"}`, true},
		{"response.reasoning_summary_text.done", `{}`, true},
		{"response.reasoning_text.delta", `{"delta":"x"}`, true},
		{"response.reasoning_text.done", `{}`, true},
		{"response.output_text.delta", `{"delta":"x"}`, false},
		{"response.function_call_arguments.delta", `{"delta":"x"}`, false},
		{"response.completed", `{}`, false},
		{"response.failed", `{}`, false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, openAIStreamEventIsReasoningPrelude(tc.data, tc.eventType), "type=%s data=%s", tc.eventType, tc.data)
	}
	require.True(t, openAIStreamEventIsMetadata("response.metadata"))
	require.False(t, openAIStreamDataStartsClientOutput(`{"type":"response.metadata","metadata":{}}`, "response.metadata"))
	require.False(t, openAIStreamDataStartsSemanticTTFT(`{"type":"response.metadata","metadata":{}}`, "response.metadata"))
}

// 透传路径（生产 codex 组未使用）只吃到 metadata 修正，不做推理暂存。
func TestOpenAIPassthroughMetadataBeforeFailureFailsOver(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(reasoningHoldCreated + reasoningHoldInProgress + reasoningHoldMetadata + reasoningHoldFailed)),
		Header:     http.Header{"X-Request-Id": []string{"rid-passthrough-metadata"}},
	}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Name: "acc"}
	_, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "model", "model")
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.NotContains(t, rec.Body.String(), "data:")
}
