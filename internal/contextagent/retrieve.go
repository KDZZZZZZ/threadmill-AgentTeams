package contextagent

import (
	"context"
	"regexp"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type ContextRetrieveRequest struct {
	Query string `json:"query"`
}

type ContextRetrieveResult struct {
	Slice           contextgraph.ContextSliceDelta `json:"slice"`
	SubscriptionIDs []string                       `json:"subscription_ids"`
	Explanation     string                         `json:"explanation"`
}

type Agent struct {
	Searcher contextgraph.ContextGraphSearcher
}

type RetrieveInvocationDispatcher interface {
	Retrieve(context.Context, auth.Principal, ContextRetrieveRequest) (ContextRetrieveResult, error)
}

func (a Agent) Retrieve(ctx context.Context, principal auth.Principal, req ContextRetrieveRequest) (ContextRetrieveResult, error) {
	if err := ValidateRetrieveRequest(req); err != nil {
		return ContextRetrieveResult{}, err
	}
	if err := requireTrustedRetrievePrincipal(principal); err != nil {
		return ContextRetrieveResult{}, err
	}
	searchReq := BuildSearchRequest(req.Query)
	if len(searchReq.Keywords) == 0 && len(searchReq.Scope) == 0 && len(searchReq.AnchorRefs) == 0 {
		return ContextRetrieveResult{}, kernel.InvalidArgument("query did not contain a bounded searchable entity, constraint, module, or relation")
	}
	result, err := a.Searcher.Search(ctx, principal, searchReq)
	if err != nil {
		return ContextRetrieveResult{}, err
	}
	return ContextRetrieveResult{
		Slice:           result.Slice,
		SubscriptionIDs: append([]string(nil), result.SubscriptionIDs...),
		Explanation:     explain(searchReq),
	}, nil
}

func ValidateRetrieveRequest(req ContextRetrieveRequest) error {
	if strings.TrimSpace(req.Query) == "" {
		return kernel.InvalidArgument("query is required")
	}
	return nil
}

func requireTrustedRetrievePrincipal(principal auth.Principal) error {
	if principal.Role != auth.RoleContext || principal.Operation != "retrieve" {
		return kernel.Forbidden("context retrieve requires Context Agent retrieve principal")
	}
	if !principal.HasTool(auth.ToolContextSearch) {
		return kernel.Forbidden("context retrieve requires context.search capability")
	}
	if principal.ConsumerInvocationID == "" {
		return kernel.InvalidArgument("context retrieve requires trusted consumer invocation")
	}
	return nil
}

var tokenPattern = regexp.MustCompile(`[\p{L}\p{N}_.:/-]+`)

var stopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "by": {}, "for": {},
	"from": {}, "in": {}, "is": {}, "of": {}, "on": {}, "or": {}, "the": {},
	"to": {}, "with": {}, "about": {}, "find": {}, "search": {}, "retrieve": {},
	"near": {},
	"请":    {}, "查找": {}, "搜索": {}, "检索": {}, "关于": {}, "有关": {},
	"相关": {}, "哪些": {}, "什么": {}, "是否": {}, "的": {}, "和": {},
	"或": {}, "与": {}, "在": {}, "中": {}, "里": {}, "节点": {}, "上下文": {},
}

func BuildSearchRequest(query string) contextgraph.SearchRequest {
	tokens := tokenPattern.FindAllString(normalizeRetrieveQuery(query), -1)
	seenKeywords := map[string]struct{}{}
	seenScope := map[string]struct{}{}
	seenAnchors := map[string]struct{}{}
	var req contextgraph.SearchRequest
	for _, token := range tokens {
		normalized := strings.ToLower(strings.Trim(token, ".,;()[]{}\"'"))
		if normalized == "" {
			continue
		}
		if strings.HasPrefix(normalized, "node:") || strings.HasPrefix(normalized, "subgraph:") {
			if _, ok := seenAnchors[normalized]; !ok {
				seenAnchors[normalized] = struct{}{}
				req.AnchorRefs = append(req.AnchorRefs, normalized)
			}
			if strings.HasPrefix(normalized, "subgraph:") {
				if _, ok := seenScope[normalized]; !ok {
					seenScope[normalized] = struct{}{}
					req.Scope = append(req.Scope, normalized)
				}
			}
			continue
		}
		if _, stop := stopWords[normalized]; stop {
			continue
		}
		if _, ok := seenKeywords[normalized]; ok {
			continue
		}
		seenKeywords[normalized] = struct{}{}
		req.Keywords = append(req.Keywords, normalized)
	}
	return req
}

func normalizeRetrieveQuery(query string) string {
	// Chinese intent words are frequently attached directly to the subject.
	// Split only the navigation boilerplate; keep domain phrases intact so the
	// mechanical substring search remains bounded and explainable.
	replacer := strings.NewReplacer(
		"请帮我", " ", "帮我", " ", "查找关于", " ", "搜索关于", " ", "检索关于", " ",
		"上下文图中的", " ", "上下文图中", " ", "上下文图", " ", "上下文节点", " ", "上下文", " ", "相关的", " ",
		"请", " ", "查找", " ", "搜索", " ", "检索", " ", "关于", " ", "有关", " ",
		"哪些", " ", "什么", " ", "是否", " ", "的", " ", "和", " ", "或", " ", "与", " ",
	)
	return strings.TrimSpace(replacer.Replace(query))
}

func explain(req contextgraph.SearchRequest) string {
	parts := []string{"translated natural-language query to context.search"}
	if len(req.Keywords) > 0 {
		parts = append(parts, "keywords="+strings.Join(req.Keywords, ","))
	}
	if len(req.Scope) > 0 {
		parts = append(parts, "scope="+strings.Join(req.Scope, ","))
	}
	if len(req.AnchorRefs) > 0 {
		parts = append(parts, "anchors="+strings.Join(req.AnchorRefs, ","))
	}
	return strings.Join(parts, "; ")
}
