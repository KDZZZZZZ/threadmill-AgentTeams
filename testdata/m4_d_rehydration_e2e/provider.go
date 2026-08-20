//go:build ignore

// Deterministic OpenAI-compatible provider for the focused M4-E2 receipt path.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
)

var (
	taskPattern   = regexp.MustCompile(`tm-phase-[A-Za-z0-9_-]+-g[0-9]+-e[0-9]+`)
	digestPattern = regexp.MustCompile(`[a-f0-9]{64}`)
)

type chatRequest struct {
	Messages []map[string]any `json:"messages"`
	Input    any              `json:"input"`
	Tools    []struct {
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	} `json:"tools"`
}

func main() {
	http.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]string{{"id": "qwen-plus"}}})
	})
	http.HandleFunc("/v1/chat/completions", chat)
	if err := http.ListenAndServe(":18092", nil); err != nil {
		panic(err)
	}
}

func chat(w http.ResponseWriter, r *http.Request) {
	var request chatRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	// QwenPaw may use either the Chat Completions messages shape or an
	// OpenAI-compatible input envelope. Inspect the complete agent-visible
	// request, never headers or credentials.
	raw, _ := json.Marshal(request)
	text := string(raw)
	taskID := taskPattern.FindString(text)
	digest := digestPattern.FindString(text)
	taskflow, receipt := toolName(request, "taskflow"), toolName(request, "confirmPackageConsumption")

	name, args, callID := "", "", ""
	stage := "done"
	hasFullSpec := strings.Contains(text, "Required continuation handshake") || (strings.Contains(text, `task_contract`) && strings.Contains(text, `input_revision`) && strings.Contains(text, `newly_delivered_inputs`))
	if strings.Contains(text, `"recorded_at"`) || strings.Contains(text, `"session_identity":"matrix:`) {
		// The authenticated Runtime receipt is already in this fresh session.
	} else if hasFullSpec && receipt != "" && digest != "" {
		name, args, callID = receipt, fmt.Sprintf(`{"package_digest":%q,"consumed":true}`, digest), "call-confirm-package-consumption"
		stage = "receipt"
	} else if taskflow != "" && taskID != "" {
		name, args, callID = taskflow, fmt.Sprintf(`{"action":"ack_task","role":"worker","payload":{"taskId":%q}}`, taskID), "call-ack-full-spec"
		stage = "ack"
	} else {
		stage = "no-action"
	}
	trace(stage, taskID != "", digest != "", taskflow != "", receipt != "", hasFullSpec, strings.Contains(text, `"action":"ack_task"`), strings.Contains(text, `"ok":true`), strings.Contains(text, `"spec"`), strings.Contains(strings.ToLower(text), "error"))

	w.Header().Set("Content-Type", "text/event-stream")
	if name == "" {
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"continuation package confirmed\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		return
	}
	fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":%q,\"type\":\"function\",\"function\":{\"name\":%q,\"arguments\":%q}}]},\"finish_reason\":null}]}\n\n", callID, name, args)
	fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
}

func trace(stage string, hasTask, hasDigest, hasTaskflow, hasReceipt, hasFullSpec, hasAckCall, hasOK, hasSpec, hasError bool) {
	path := os.Getenv("M4E2_PROVIDER_TRACE")
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_ = json.NewEncoder(file).Encode(map[string]any{"stage": stage, "hasTask": hasTask, "hasDigest": hasDigest, "hasTaskflow": hasTaskflow, "hasReceipt": hasReceipt, "hasFullSpec": hasFullSpec, "hasAckCall": hasAckCall, "hasOK": hasOK, "hasSpec": hasSpec, "hasError": hasError})
}

func toolName(request chatRequest, fragment string) string {
	for _, tool := range request.Tools {
		if strings.Contains(tool.Function.Name, fragment) {
			return tool.Function.Name
		}
	}
	return ""
}
