package promptcatalog

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const MissingValue = "未提供"

type Asset struct {
	ID     string
	Body   string
	SHA256 string
}

type Skill struct {
	ID           string
	Description  string
	Dependencies []string
	Tools        []auth.Tool
	Body         string
	SHA256       string
}

type Catalog struct {
	Prompts        map[string]Asset
	Skills         map[string]Skill
	availableTools map[auth.Tool]struct{}
}

type Bundle struct {
	Role           auth.Role
	Operation      string
	SharedPrompt   Asset
	RolePrompt     Asset
	Skills         []Skill
	EffectiveTools []auth.Tool
}

type RenderData struct {
	RuntimeEnvelope    string
	BoundaryInput      string
	StartOrResumeInput string
	TaskContract       string
	PhaseSpec          string
	WorkspaceBinding   string
	ContextSlice       string
	TaskMemoryBuffer   string
	RepositoryPolicy   string
	LatestEvents       string
}

type Rendered struct {
	Text         string
	PromptHashes map[string]string
	SkillHashes  map[string]string
	SHA256       string
}

var requiredSkills = []string{
	"candidate-review",
	"context-navigation",
	"context-retrieval-request",
	"context-semantic-retrieval",
	"context-subscription",
	"coordination-control",
	"execution-delivery",
	"general-context-curation",
	"orchestration-escalation",
	"phase-runtime",
	"phase-submit",
	"planning-delivery",
	"task-context-lifecycle",
	"task-memory",
	"verification-delivery",
}

var promptFiles = map[string]string{
	"shared":        "shared.md",
	"task_manager":  "task-manager.md",
	"context_agent": "context-agent.md",
	"planner":       "planner.md",
	"executor":      "executor.md",
	"verifier":      "verifier.md",
}

func Load(repoRoot string, availableTools map[auth.Tool]struct{}) (*Catalog, error) {
	if strings.TrimSpace(repoRoot) == "" {
		return nil, fmt.Errorf("repo root is required")
	}
	if len(availableTools) == 0 {
		return nil, fmt.Errorf("runtime available tools are required")
	}
	catalog := &Catalog{
		Prompts:        make(map[string]Asset, len(promptFiles)),
		Skills:         make(map[string]Skill, len(requiredSkills)),
		availableTools: cloneToolSet(availableTools),
	}
	for id, name := range promptFiles {
		body, err := os.ReadFile(filepath.Join(repoRoot, "runtime-assets", "prompts", name))
		if err != nil {
			return nil, fmt.Errorf("load required prompt %s: %w", id, err)
		}
		catalog.Prompts[id] = Asset{ID: id, Body: string(body), SHA256: hash(body)}
	}
	for _, id := range requiredSkills {
		body, err := os.ReadFile(filepath.Join(repoRoot, "docs", "agent-skills", id, "SKILL.md"))
		if err != nil {
			return nil, fmt.Errorf("load required skill %s: %w", id, err)
		}
		skill, err := parseSkill(body)
		if err != nil {
			return nil, fmt.Errorf("parse skill %s: %w", id, err)
		}
		if skill.ID != id {
			return nil, fmt.Errorf("skill directory %s declares name %s", id, skill.ID)
		}
		for _, tool := range skill.Tools {
			if !auth.IsCanonicalTool(tool) {
				return nil, fmt.Errorf("skill %s declares unknown tool %s", id, tool)
			}
			if _, ok := availableTools[tool]; !ok {
				return nil, fmt.Errorf("skill %s requires unavailable tool %s", id, tool)
			}
		}
		catalog.Skills[id] = skill
	}
	if _, err := catalog.ResolveSkills(requiredSkills); err != nil {
		return nil, err
	}
	return catalog, nil
}

func (c *Catalog) ResolveSkills(ids []string) ([]Skill, error) {
	state := make(map[string]uint8, len(c.Skills))
	resolved := make([]Skill, 0, len(ids))
	var visit func(string) error
	visit = func(id string) error {
		skill, ok := c.Skills[id]
		if !ok {
			return fmt.Errorf("unknown skill %s", id)
		}
		switch state[id] {
		case 1:
			return fmt.Errorf("skill dependency cycle at %s", id)
		case 2:
			return nil
		}
		state[id] = 1
		for _, dependency := range skill.Dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[id] = 2
		resolved = append(resolved, skill)
		return nil
	}
	for _, id := range ids {
		if err := visit(id); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

func (c *Catalog) Bundle(role auth.Role, operation string) (Bundle, error) {
	direct, err := directSkills(role, operation)
	if err != nil {
		return Bundle{}, err
	}
	skills, err := c.ResolveSkills(direct)
	if err != nil {
		return Bundle{}, err
	}
	rolePrompt, ok := c.Prompts[string(role)]
	if !ok {
		return Bundle{}, fmt.Errorf("missing role prompt for %s", role)
	}
	roleTools := auth.InvocationCapabilityTools(role, operation)
	skillTools := make(map[auth.Tool]struct{})
	for _, skill := range skills {
		for _, tool := range skill.Tools {
			if _, ok := roleTools[tool]; !ok {
				return Bundle{}, fmt.Errorf("skill %s tool %s is outside role %s capability", skill.ID, tool, role)
			}
			skillTools[tool] = struct{}{}
		}
	}
	tools := intersectTools(roleTools, skillTools, c.availableTools)
	return Bundle{
		Role:           role,
		Operation:      operation,
		SharedPrompt:   c.Prompts["shared"],
		RolePrompt:     rolePrompt,
		Skills:         skills,
		EffectiveTools: tools,
	}, nil
}

func (b Bundle) Render(data RenderData) Rendered {
	skillBodies := make([]string, 0, len(b.Skills))
	skillIDs := make([]string, 0, len(b.Skills))
	toolIDs := make([]string, 0, len(b.EffectiveTools))
	skillHashes := make(map[string]string, len(b.Skills))
	for _, skill := range b.Skills {
		skillBodies = append(skillBodies, skill.Body)
		skillIDs = append(skillIDs, skill.ID)
		skillHashes[skill.ID] = skill.SHA256
	}
	for _, tool := range b.EffectiveTools {
		toolIDs = append(toolIDs, string(tool))
	}
	values := map[string]string{
		"{{RUNTIME_ENVELOPE}}":      present(data.RuntimeEnvelope),
		"{{BOUNDARY_INPUT}}":        present(data.BoundaryInput),
		"{{START_OR_RESUME_INPUT}}": present(data.StartOrResumeInput),
		"{{TASK_CONTRACT}}":         present(data.TaskContract),
		"{{PHASE_SPEC}}":            present(data.PhaseSpec),
		"{{WORKSPACE_BINDING}}":     present(data.WorkspaceBinding),
		"{{CONTEXT_SLICE}}":         present(data.ContextSlice),
		"{{TASK_MEMORY_BUFFER}}":    present(data.TaskMemoryBuffer),
		"{{REPOSITORY_POLICIES}}":   present(data.RepositoryPolicy),
		"{{LATEST_RUNTIME_EVENTS}}": present(data.LatestEvents),
		"{{LOADED_SKILLS}}":         strings.Join(skillIDs, ", "),
		"{{AVAILABLE_TOOLS}}":       strings.Join(toolIDs, ", "),
	}
	text := strings.Join(append([]string{b.SharedPrompt.Body, b.RolePrompt.Body}, skillBodies...), "\n\n---\n\n")
	for placeholder, value := range values {
		text = strings.ReplaceAll(text, placeholder, value)
	}
	return Rendered{
		Text: text,
		PromptHashes: map[string]string{
			"shared":       b.SharedPrompt.SHA256,
			string(b.Role): b.RolePrompt.SHA256,
		},
		SkillHashes: skillHashes,
		SHA256:      hash([]byte(text)),
	}
}

func parseSkill(body []byte) (Skill, error) {
	skill := Skill{Body: string(body), SHA256: hash(body)}
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	section := ""
	inFrontMatter := false
	frontMatterSeen := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "---" {
			if !frontMatterSeen {
				frontMatterSeen = true
				inFrontMatter = true
			} else if inFrontMatter {
				inFrontMatter = false
			}
			continue
		}
		if inFrontMatter {
			if value, ok := cutField(line, "name:"); ok {
				skill.ID = value
			}
			if value, ok := cutField(line, "description:"); ok {
				skill.Description = value
			}
			continue
		}
		if strings.HasPrefix(line, "## ") {
			section = strings.TrimSpace(strings.TrimPrefix(line, "## "))
			continue
		}
		if !strings.HasPrefix(line, "- `") || !strings.HasSuffix(line, "`") {
			continue
		}
		value := strings.TrimSuffix(strings.TrimPrefix(line, "- `"), "`")
		switch section {
		case "依赖":
			skill.Dependencies = append(skill.Dependencies, value)
		case "工具":
			skill.Tools = append(skill.Tools, auth.Tool(value))
		}
	}
	if err := scanner.Err(); err != nil {
		return Skill{}, err
	}
	if skill.ID == "" || skill.Description == "" {
		return Skill{}, fmt.Errorf("skill front matter requires name and description")
	}
	return skill, nil
}

func directSkills(role auth.Role, operation string) ([]string, error) {
	switch role {
	case auth.RoleTaskManager:
		if operation != "" {
			return nil, fmt.Errorf("task manager does not accept operation %q", operation)
		}
		return []string{"context-navigation", "context-subscription", "context-retrieval-request", "coordination-control", "task-context-lifecycle"}, nil
	case auth.RoleContext:
		switch operation {
		case "retrieve":
			return []string{"context-navigation", "context-semantic-retrieval"}, nil
		case "curate":
			return []string{"context-navigation", "general-context-curation"}, nil
		case "review":
			return []string{"context-navigation", "candidate-review"}, nil
		default:
			return nil, fmt.Errorf("context agent requires retrieve, curate, or review operation")
		}
	case auth.RolePlanner:
		return phaseSkills("planning-delivery", operation)
	case auth.RoleExecutor:
		return phaseSkills("execution-delivery", operation)
	case auth.RoleVerifier:
		return phaseSkills("verification-delivery", operation)
	default:
		return nil, fmt.Errorf("unsupported runtime role %s", role)
	}
}

func phaseSkills(deliverySkill, operation string) ([]string, error) {
	if operation != "" {
		return nil, fmt.Errorf("phase role does not accept operation %q", operation)
	}
	return []string{
		"context-navigation",
		"context-subscription",
		"context-retrieval-request",
		"phase-runtime",
		"orchestration-escalation",
		"task-memory",
		"phase-submit",
		deliverySkill,
	}, nil
}

func intersectTools(sets ...map[auth.Tool]struct{}) []auth.Tool {
	if len(sets) == 0 {
		return nil
	}
	result := make([]auth.Tool, 0, len(sets[0]))
	for tool := range sets[0] {
		present := true
		for _, set := range sets[1:] {
			if _, ok := set[tool]; !ok {
				present = false
				break
			}
		}
		if present {
			result = append(result, tool)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func cloneToolSet(input map[auth.Tool]struct{}) map[auth.Tool]struct{} {
	cloned := make(map[auth.Tool]struct{}, len(input))
	for tool := range input {
		cloned[tool] = struct{}{}
	}
	return cloned
}

func cutField(line, prefix string) (string, bool) {
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}
	return strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, prefix)), `"`), true
}

func present(value string) string {
	if strings.TrimSpace(value) == "" {
		return MissingValue
	}
	return value
}

func hash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
