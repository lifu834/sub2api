package service

import (
	"strings"

	"github.com/tidwall/gjson"
)

// NYCATAI: 正文/工具调用开始之前的推理事件先暂存，不提交给客户端。
//
// 上游（尤其是 ChatGPT 后端的中转）常在吐完推理项后才因过载以 response.failed
// 收尾。原逻辑把推理事件当成"已输出"，一旦提交，failover 就被抑制，客户端拿到
// 200 空回。推理项只是正文之前的准备阶段：暂存后若失败可以安全换号重试——客户端
// 没收到任何东西，也不会把另一个号产出的 encrypted_content 带进下一轮。
//
// 代价：客户端看不到推理摘要的逐字流式，要等正文或工具调用开始时一并收到。
// 暂存期间下游照常收到 keepalive 注释，不会触发中间层空闲超时。

// openAIReasoningPreludeHoldMaxBytes 是推理暂存的上限，取首输出暂存上限的一半。
// 超过后不再暂存、按原逻辑提交，避免把一个正常进行中的长推理变成暂存超限 failover。
const openAIReasoningPreludeHoldMaxBytes = openAIFirstOutputStageMaxBytes / 2

// openAIStreamEventIsReasoningPrelude 判断事件是否只属于推理项。
func openAIStreamEventIsReasoningPrelude(data, eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "response.reasoning_summary_part.added",
		"response.reasoning_summary_part.done",
		"response.reasoning_summary_text.delta",
		"response.reasoning_summary_text.done",
		"response.reasoning_text.delta",
		"response.reasoning_text.done":
		return true
	case "response.output_item.added", "response.output_item.done":
		return strings.TrimSpace(gjson.Get(data, "item.type").String()) == "reasoning"
	default:
		return false
	}
}
