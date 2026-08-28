package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// KindLLM is a model call made on the agent's behalf.
//
// The agent does not call the model itself, for the same reason it does not run commands itself:
// a run that dies has to walk back to exactly the state it was in, and it cannot do that if its
// reasoning is regenerated on the way. A fixture agent is deterministic and replays for free, but
// a real model is not — resumed, it would sample different completions, ask for different steps,
// and make replay-by-sequence-number meaningless. Routing model calls through here checkpoints the
// completion alongside every other step, so a resumed agent is handed back the exact tokens it saw
// the first time and arrives at the same place.
//
// The cost argument points the same way: a replayed run pays for its tokens once, not once per
// restart.
const KindLLM = "llm"

// LLMInput is the payload of a KindLLM step. It is deliberately close to the wire shape of the
// Messages API rather than an abstraction over it — an agent that wants to say something to the
// model should not have to learn a second vocabulary first.
type LLMInput struct {
	System    string       `json:"system,omitempty"`
	Messages  []LLMMessage `json:"messages"`
	MaxTokens int64        `json:"max_tokens,omitempty"`
	Model     string       `json:"model,omitempty"`
}

type LLMMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// LLMOutput is what gets checkpointed, and therefore what a resumed agent is replayed.
type LLMOutput struct {
	Text       string `json:"text"`
	StopReason string `json:"stop_reason,omitempty"`
	Model      string `json:"model,omitempty"`

	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// LLM executes model calls against the Claude API.
type LLM struct {
	client       anthropic.Client
	defaultModel string
	maxTokens    int64
}

// NewLLM builds an executor for model steps. The API key comes from the environment the way the
// SDK expects, so nothing here handles a credential.
func NewLLM(defaultModel string, maxTokens int64, opts ...option.RequestOption) *LLM {
	return &LLM{
		client:       anthropic.NewClient(opts...),
		defaultModel: defaultModel,
		maxTokens:    maxTokens,
	}
}

func (l *LLM) Execute(ctx context.Context, req Request) (json.RawMessage, error) {
	if req.Kind != KindLLM {
		return nil, fmt.Errorf("llm executor got step kind %q", req.Kind)
	}
	var in LLMInput
	if err := json.Unmarshal(req.Input, &in); err != nil {
		return nil, fmt.Errorf("decode llm input: %w", err)
	}
	if len(in.Messages) == 0 {
		return nil, fmt.Errorf("llm step has no messages")
	}

	messages := make([]anthropic.MessageParam, 0, len(in.Messages))
	for i, m := range in.Messages {
		block := anthropic.NewTextBlock(m.Content)
		switch m.Role {
		case "user":
			messages = append(messages, anthropic.NewUserMessage(block))
		case "assistant":
			messages = append(messages, anthropic.NewAssistantMessage(block))
		default:
			return nil, fmt.Errorf("llm message %d has unknown role %q", i, m.Role)
		}
	}

	params := anthropic.MessageNewParams{
		Model:     anthropic.Model(firstNonEmpty(in.Model, l.defaultModel)),
		MaxTokens: firstNonZero(in.MaxTokens, l.maxTokens),
		Messages:  messages,
	}
	if in.System != "" {
		params.System = []anthropic.TextBlockParam{{Text: in.System}}
	}

	// No retry loop here on purpose. A model call that fails is a failed step, and a failed step is
	// already the control plane's problem to retry — adding a second retry policy inside the
	// executor would mean two components backing off against each other.
	resp, err := l.client.Messages.New(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("messages: %w", err)
	}

	var text string
	for _, block := range resp.Content {
		if t, ok := block.AsAny().(anthropic.TextBlock); ok {
			text += t.Text
		}
	}

	return json.Marshal(LLMOutput{
		Text:         text,
		StopReason:   string(resp.StopReason),
		Model:        string(resp.Model),
		InputTokens:  resp.Usage.InputTokens,
		OutputTokens: resp.Usage.OutputTokens,
	})
}

func (l *LLM) Close() error { return nil }

func firstNonEmpty(chosen, fallback string) string {
	if chosen != "" {
		return chosen
	}
	return fallback
}

func firstNonZero(chosen, fallback int64) int64 {
	if chosen != 0 {
		return chosen
	}
	return fallback
}
