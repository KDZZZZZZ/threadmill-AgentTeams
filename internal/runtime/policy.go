package runtime

import (
	"fmt"
	"sort"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
)

type ToolSource struct {
	RoleTools      map[auth.Tool]struct{}
	SkillTools     map[auth.Tool]struct{}
	AvailableTools map[auth.Tool]struct{}
}

func EffectiveTools(source ToolSource) ([]auth.Tool, error) {
	if len(source.RoleTools) == 0 || len(source.SkillTools) == 0 || len(source.AvailableTools) == 0 {
		return nil, fmt.Errorf("role, skill, and runtime tool sets are all required")
	}
	tools := make([]auth.Tool, 0)
	for tool := range source.RoleTools {
		if _, ok := source.SkillTools[tool]; !ok {
			continue
		}
		if _, ok := source.AvailableTools[tool]; !ok {
			continue
		}
		tools = append(tools, tool)
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i] < tools[j] })
	if len(tools) == 0 {
		return nil, fmt.Errorf("effective tool intersection is empty")
	}
	return tools, nil
}

type Assembler struct {
	catalog *promptcatalog.Catalog
}

func NewAssembler(catalog *promptcatalog.Catalog) (*Assembler, error) {
	if catalog == nil {
		return nil, fmt.Errorf("prompt catalog is required")
	}
	return &Assembler{catalog: catalog}, nil
}

type Assembly struct {
	Invocation Invocation
	Prompt     promptcatalog.Rendered
}

func (a *Assembler) Assemble(invocation Invocation, data promptcatalog.RenderData) (Assembly, error) {
	bundle, err := a.catalog.Bundle(invocation.Role, invocation.Operation)
	if err != nil {
		return Assembly{}, err
	}
	rendered := bundle.Render(data)
	invocation.PromptHashes = cloneStringMap(rendered.PromptHashes)
	invocation.SkillHashes = cloneStringMap(rendered.SkillHashes)
	invocation.EffectiveTools = append([]auth.Tool(nil), bundle.EffectiveTools...)
	if err := invocation.Validate(); err != nil {
		return Assembly{}, err
	}
	return Assembly{Invocation: invocation, Prompt: rendered}, nil
}
