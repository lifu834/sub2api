package handler

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// NYCATAI 补丁 261121：官方 0.2.24 尚未覆盖的 Codex 注入上下文。
var nycataiCodexContextNamesForTest = []string{"terminal_info", "browser_tabs"}

func requireFunctionCallOutputRejected(t *testing.T, body []byte) {
	t.Helper()
	h := &OpenAIGatewayHandler{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	require.False(t, h.validateFunctionCallOutputRequest(c, body, zap.NewNop()))
	require.Equal(t, 400, w.Code)
	require.Contains(t, w.Body.String(), "function_call_output requires call_id")
}

func requireFunctionCallOutputAccepted(t *testing.T, body []byte) {
	t.Helper()
	h := &OpenAIGatewayHandler{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	require.True(t, h.validateFunctionCallOutputRequest(c, body, zap.NewNop()))
	require.False(t, c.Writer.Written())
}

func TestNormalizeCodexNycataiContextNames(t *testing.T) {
	for _, name := range nycataiCodexContextNamesForTest {
		t.Run(name, func(t *testing.T) {
			output := "retest context\nline two"
			// #8991 的原始形态不带 id，#8999 的带 id；两种都要覆盖。
			for _, withID := range []bool{false, true} {
				item := map[string]any{"type": "function_call_output", "namespace": "agent", "name": name, "output": output}
				if withID {
					item["id"] = "fco_example"
				}
				body := pageContextRequest(t, item, map[string]any{"role": "user", "content": "continue"})
				requireFunctionCallOutputRejected(t, body)

				got, changed := normalizeCodexUntrustedContext(body)
				require.True(t, changed)
				requireFunctionCallOutputAccepted(t, got)
				first := gjson.GetBytes(got, "input.0")
				require.Equal(t, "message", first.Get("type").String())
				require.Equal(t, "user", first.Get("role").String())
				require.Equal(t, "<external_"+name+">"+output+"</external_"+name+">", first.Get("content.0.text").String())
				require.False(t, first.Get("call_id").Exists())
				require.False(t, first.Get("id").Exists())
				require.Equal(t, "continue", gjson.GetBytes(got, "input.1.content").String())

				again, changedAgain := normalizeCodexUntrustedContext(got)
				require.False(t, changedAgain)
				require.Equal(t, got, again)
			}
		})
	}
}

func TestNormalizeCodexNycataiContextEscapedName(t *testing.T) {
	// 名字用 \u 转义拼写时，字面量预检会漏掉，必须靠转义探测走到完整解析。
	body := []byte(`{"model":"gpt-5","input":[{"type":"function_call_output","namespace":"agent","name":"terminal_info","output":"pwd: /repo"}]}`)
	got, changed := normalizeCodexUntrustedContext(body)
	require.True(t, changed)
	require.Equal(t, "<external_terminal_info>pwd: /repo</external_terminal_info>", gjson.GetBytes(got, "input.0.content.0.text").String())
}

func TestNormalizeCodexNycataiContextKeepsStrictBoundaries(t *testing.T) {
	for _, name := range nycataiCodexContextNamesForTest {
		for _, override := range []map[string]any{
			{"call_id": "call_real"},
			{"call_id": nil},
			{"call_id": 42},
			{"namespace": "other"},
			{"namespace": "codex_app"},
			{"output": nil},
			{"output": ""},
			{"output": " \t\n"},
			{"output": []any{map[string]any{"type": "input_text", "text": "x"}}},
			{"type": "custom_tool_call_output"},
		} {
			item := pageContextItem(name, "context")
			for key, value := range override {
				item[key] = value
			}
			body := pageContextRequest(t, item)
			got, changed := normalizeCodexUntrustedContext(body)
			require.False(t, changed, "name=%s override=%v", name, override)
			require.Equal(t, body, got)
		}
	}

	// 未登记的名字仍按严格校验拒绝，不做命名空间通配放行。
	unknown := pageContextRequest(t, pageContextItem("future_context", "context"))
	got, changed := normalizeCodexUntrustedContext(unknown)
	require.False(t, changed)
	requireFunctionCallOutputRejected(t, got)

	// 上下文项被改写后，不能让同一请求里的真实工具结果绕过配对校验。
	mixed := pageContextRequest(t, pageContextItem("terminal_info", "context"), pageContextItem("read_file", "result"))
	got, changed = normalizeCodexUntrustedContext(mixed)
	require.True(t, changed)
	require.True(t, service.ValidateFunctionCallOutputContextBytes(got).HasFunctionCallOutputMissingCallID)
	requireFunctionCallOutputRejected(t, got)
}

func TestNormalizeCodexNycataiContextAfterCompaction(t *testing.T) {
	// 长会话压缩后历史里没有任何调用项，只剩消息、压缩项与注入的上下文项。
	body := []byte(`{"model":"gpt-6.2-sol","input":[` +
		`{"type":"message","role":"developer","content":[{"type":"input_text","text":"rules"}]},` +
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"task"}]},` +
		`{"type":"compaction","encrypted_content":"sealed"},` +
		`{"type":"function_call_output","namespace":"agent","name":"terminal_info","output":"shell: pwsh"},` +
		`{"type":"function_call_output","namespace":"agent","name":"ide_context","output":"# Context from my IDE setup:"},` +
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}]}`)
	requireFunctionCallOutputRejected(t, body)

	got, changed := normalizeCodexUntrustedContext(body)
	require.True(t, changed)
	requireFunctionCallOutputAccepted(t, got)
	items := gjson.GetBytes(got, "input").Array()
	require.Len(t, items, 6)
	require.Equal(t, "compaction", items[2].Get("type").String())
	require.Equal(t, "sealed", items[2].Get("encrypted_content").String())
	require.Equal(t, "<external_terminal_info>shell: pwsh</external_terminal_info>", items[3].Get("content.0.text").String())
	require.Equal(t, "user", items[4].Get("role").String())
	require.Equal(t, "continue", items[5].Get("content.0.text").String())
}

func TestCodexCallOutputRejectFieldsLogsNamesWithoutContent(t *testing.T) {
	const secret = "SECRET-OUTPUT-should-not-be-logged"
	body := []byte(`{"model":"gpt-5","input":[` +
		`{"type":"message","role":"user","content":"hi"},` +
		`{"type":"compaction","encrypted_content":"sealed"},` +
		`{"type":"function_call_output","namespace":"agent","name":"future_context","output":"` + secret + `"},` +
		`{"type":"tool_search_output","call_id":null,"execution":"server","tools":[]}]}`)

	core, logs := observer.New(zapcore.WarnLevel)
	h := &OpenAIGatewayHandler{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	require.False(t, h.validateFunctionCallOutputRequest(c, body, zap.New(core)))
	require.Equal(t, 400, w.Code)

	entries := logs.FilterMessage("openai.request_validation_failed").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	require.Equal(t, "function_call_output_missing_call_id", fields["reason"])
	require.EqualValues(t, 4, fields["input_items"])
	require.Equal(t, []any{"compaction=1", "function_call_output=1", "message=1", "tool_search_output=1"}, fields["input_item_types"])
	require.Equal(t, []any{"function_call_output|agent|future_context", "tool_search_output||"}, fields["missing_call_id_outputs"])
	for _, entry := range entries {
		for _, field := range entry.Context {
			require.NotContains(t, field.String, secret)
		}
	}
	require.NotContains(t, strings.Join(func() []string {
		var all []string
		for key, value := range fields {
			all = append(all, key, toTestString(value))
		}
		return all
	}(), " "), secret)
}

func TestCodexRejectDiagFieldSanitizesAndTruncates(t *testing.T) {
	require.Equal(t, "a_b_c_d", codexRejectDiagField("a|b=c\nd"))
	long := strings.Repeat("中", 30) // 90 字节
	got := codexRejectDiagField(long)
	require.LessOrEqual(t, len(got), codexRejectDiagMaxFieldBytes)
	require.Equal(t, strings.Repeat("中", 21), got)

	counts := make(map[string]int)
	for i := 0; i < codexRejectDiagMaxTypes+5; i++ {
		countCodexRejectDiagType(counts, "t"+strings.Repeat("x", i))
	}
	require.Len(t, counts, codexRejectDiagMaxTypes+1)
	require.Equal(t, 5, counts["(other)"])

	require.Equal(t, []zap.Field{zap.Bool("input_is_array", false)}, codexCallOutputRejectFields([]byte(`{"input":"text"}`)))
}

func toTestString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			parts = append(parts, toTestString(item))
		}
		return strings.Join(parts, ",")
	default:
		return ""
	}
}
