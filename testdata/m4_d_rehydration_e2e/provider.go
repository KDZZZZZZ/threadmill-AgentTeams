//go:build ignore

// Deterministic OpenAI-compatible provider for the focused M4-E2 receipt path.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
)

var (
	taskPattern     = regexp.MustCompile(`tm-phase-[A-Za-z0-9_-]+-g[0-9]+-e[0-9]+`)
	digestPattern   = regexp.MustCompile(`[a-f0-9]{64}`)
	artifactPattern = regexp.MustCompile(`artifact-[a-f0-9]{24}`)
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
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var request chatRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	// QwenPaw may use either the Chat Completions messages shape or an
	// OpenAI-compatible input envelope. Inspect the complete agent-visible
	// request, never headers or credentials.
	text := string(raw)
	taskID := taskPattern.FindString(text)
	digest := digestPattern.FindString(text)
	taskflow, receipt := toolName(request, "taskflow"), toolName(request, "confirmPackageConsumption")
	register, submit := toolName(request, "artifact_register"), toolName(request, "agent_submitPhaseOutput")
	if register == "" {
		register = toolName(request, "artifact.register")
	}
	if submit == "" {
		submit = toolName(request, "agent.submitPhaseOutput")
	}
	artifactRef := artifactPattern.FindString(text)

	name, args, callID := "", "", ""
	stage := "done"
	hasFullSpec := strings.Contains(text, "Required continuation handshake") || (strings.Contains(text, `task_contract`) && strings.Contains(text, `input_revision`) && strings.Contains(text, `newly_delivered_inputs`))
	packageComplete := hasFullSpec && strings.Contains(text, "binding-r5") && strings.Contains(text, "input-r5") && strings.Contains(text, "task_contract") && strings.Contains(text, "phase_instruction") && strings.Contains(text, "inputs") && strings.Contains(text, "newly_delivered_inputs") && strings.Contains(text, "context") && strings.Contains(text, "task_memory") && strings.Contains(text, "workspace") && strings.Contains(text, "allowed_dirs") && strings.Contains(text, "artifact_refs") && strings.Contains(text, "event_refs") && strings.Contains(text, "evidence_refs")
	hasReceiptResult := messagesContain(request.Messages, "recorded_at") && messagesContain(request.Messages, "session_identity") && messagesContain(request.Messages, `"consumed":true`)
	if artifactRef != "" && submit != "" {
		name, args, callID = submit, fmt.Sprintf(`{"phase":"execute","delivery_refs":[],"report_ref":%q,"evidence_refs":[]}`, artifactRef), "call-submit-rehydrated-phase-output"
		stage = "phase-output"
	} else if hasReceiptResult && packageComplete && register != "" {
		name, args, callID = register, `{"controlled_path":"out/rehydration.txt","kind":"generated_report","media_type":"text/plain"}`, "call-register-rehydrated-artifact"
		stage = "artifact"
	} else if hasFullSpec && receipt != "" && digest != "" {
		name, args, callID = receipt, fmt.Sprintf(`{"package_digest":%q,"consumed":true}`, digest), "call-confirm-package-consumption"
		stage = "receipt"
	} else if taskflow != "" && taskID != "" {
		name, args, callID = taskflow, fmt.Sprintf(`{"action":"ack_task","role":"worker","payload":{"taskId":%q}}`, taskID), "call-ack-full-spec"
		stage = "ack"
	} else {
		stage = "no-action"
	}
	trace(stage, taskID != "", digest != "", taskflow != "", receipt != "", hasFullSpec, packageComplete, artifactRef != "", strings.Contains(text, `"action":"ack_task"`), strings.Contains(strings.ToLower(text), "error"), request)

	w.Header().Set("Content-Type", "text/event-stream")
	if name == "" {
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"continuation package confirmed\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		return
	}
	fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":%q,\"type\":\"function\",\"function\":{\"name\":%q,\"arguments\":%q}}]},\"finish_reason\":null}]}\n\n", callID, name, args)
	fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
}

func trace(stage string, hasTask, hasDigest, hasTaskflow, hasReceipt, hasFullSpec, packageComplete, hasArtifactRef, hasAckCall, hasError bool, request chatRequest) {
	path := os.Getenv("M4E2_PROVIDER_TRACE")
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	messages := make([]map[string]any, 0, len(request.Messages))
	for _, message := range request.Messages {
		encoded, _ := json.Marshal(message)
		body := string(encoded)
		messages = append(messages, map[string]any{
			"role":              message["role"],
			"name":              message["name"],
			"tool_call_id":      message["tool_call_id"],
			"bytes":             len(encoded),
			"has_recorded_at":   strings.Contains(body, "recorded_at"),
			"has_session_id":    strings.Contains(body, "session_identity"),
			"has_consumed_true": strings.Contains(body, `\"consumed\":true`),
			"has_is_error":      strings.Contains(body, "isError"),
		})
	}
	_ = json.NewEncoder(file).Encode(map[string]any{"stage": stage, "hasTask": hasTask, "hasDigest": hasDigest, "hasTaskflow": hasTaskflow, "hasReceipt": hasReceipt, "hasFullSpec": hasFullSpec, "packageComplete": packageComplete, "hasArtifactRef": hasArtifactRef, "hasAckCall": hasAckCall, "hasError": hasError, "messages": messages})
}

func toolName(request chatRequest, fragment string) string {
	for _, tool := range request.Tools {
		if strings.Contains(tool.Function.Name, fragment) {
			return tool.Function.Name
		}
	}
	return ""
}

func messagesContain(messages []map[string]any, fragment string) bool {
	for _, message := range messages {
		if valueContains(message, fragment) {
			return true
		}
	}
	return false
}

func valueContains(value any, fragment string) bool {
	switch typed := value.(type) {
	case string:
		return strings.Contains(typed, fragment)
	case map[string]any:
		for _, child := range typed {
			if valueContains(child, fragment) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if valueContains(child, fragment) {
				return true
			}
		}
	}
	return false
}
