package handler

import (
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// NYCATAI 补丁 261121：工具输出续链预校验拒绝时的诊断字段。
//
// Codex 会不断新增以 namespace=agent、不带 call_id 的 function_call_output 注入的上下文，
// 官方按名字白名单逐个兼容。被拒时只记录缺 call_id 的输出项的 type/namespace/name，
// 以及 input 各类型项的数量，不记录任何 output、arguments 或消息正文，
// 便于从日志直接看出是哪个新名字触发、请求里是否还剩调用项（如压缩后只剩上下文项）。
const (
	codexRejectDiagMaxItems      = 16
	codexRejectDiagMaxTypes      = 24
	codexRejectDiagMaxFieldBytes = 64
)

func codexCallOutputRejectFields(body []byte) []zap.Field {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return []zap.Field{zap.Bool("input_is_array", false)}
	}
	var unpaired []string
	typeCounts := make(map[string]int)
	total := 0
	input.ForEach(func(_, item gjson.Result) bool {
		total++
		if !item.IsObject() {
			countCodexRejectDiagType(typeCounts, "(non_object)")
			return true
		}
		typ := item.Get("type").String()
		if typ == "" {
			typ = "(no_type)"
		}
		countCodexRejectDiagType(typeCounts, typ)
		if isResponsesCallOutputType(typ) && strings.TrimSpace(item.Get("call_id").String()) == "" && len(unpaired) < codexRejectDiagMaxItems {
			unpaired = append(unpaired, codexRejectDiagField(typ)+"|"+codexRejectDiagField(item.Get("namespace").String())+"|"+codexRejectDiagField(item.Get("name").String()))
		}
		return true
	})
	types := make([]string, 0, len(typeCounts))
	for typ, count := range typeCounts {
		types = append(types, typ+"="+strconv.Itoa(count))
	}
	sort.Strings(types)
	return []zap.Field{
		zap.Int("input_items", total),
		zap.Strings("input_item_types", types),
		zap.Strings("missing_call_id_outputs", unpaired),
	}
}

func countCodexRejectDiagType(counts map[string]int, typ string) {
	typ = codexRejectDiagField(typ)
	if _, ok := counts[typ]; !ok && len(counts) >= codexRejectDiagMaxTypes {
		typ = "(other)"
	}
	counts[typ]++
}

func codexRejectDiagField(value string) string {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == '|' || r == '=' {
			return '_'
		}
		return r
	}, value)
	if len(value) > codexRejectDiagMaxFieldBytes {
		value = value[:codexRejectDiagMaxFieldBytes]
		// 防止截断落在多字节字符中间。
		for len(value) > 0 && !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
	}
	return value
}
