package handler

// NYCATAI patch (encrypted-mismatch-failover, 261124): 全组都解不开时按 400 返回上游原文。

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const nycataiMismatchClientMessage = "The upstream could not validate encrypted continuation context. Rebuild the conversation context in the client."

func nycataiMismatchFailoverTestError() *service.UpstreamFailoverError {
	return &service.UpstreamFailoverError{
		StatusCode:        http.StatusBadRequest,
		ResponseBody:      []byte(`{"error":{"type":"upstream_error","message":"generic envelope"}}`),
		Scope:             service.GatewayFailureScopeRequest,
		Reason:            service.GatewayFailureReasonNycataiEncryptedMismatch,
		NextAccountAction: service.NextAccountRetry,
		ClientStatusCode:  http.StatusBadRequest,
		ClientMessage:     nycataiMismatchClientMessage,
	}
}

func TestNycataiEncryptedMismatchExhausted_ReturnsJSON400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, nycataiMismatchFailoverTestError(), false)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
	errBody, ok := envelope["error"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "invalid_request_error", errBody["type"])
	require.Equal(t, nycataiMismatchClientMessage, errBody["message"])
}

func TestNycataiEncryptedMismatchExhausted_ReturnsResponsesSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, nycataiMismatchFailoverTestError(), true)

	body := rec.Body.String()
	require.True(t, strings.HasPrefix(body, "event: response.failed\n"), body)
	require.Contains(t, body, "continuation context")
}
