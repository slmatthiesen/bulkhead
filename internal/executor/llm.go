package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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

// LLMInput is the payload of a KindLLM step. It is deliberately close to the wire shape of a chat
// completion rather than an abstraction over it — an agent that wants to say something to a model
// should not have to learn a second vocabulary first.
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

// provider is one way of turning a prompt into a completion.
//
// Two exist because development and the scored run want different things. Only resolve rate depends
// on model quality — throughput, latency and the failure taxonomy are harness properties — so the
// harness is developed against whatever inference is cheapest and pointed at a capable model once,
// for the one number that warrants it. A provider that required the control plane to change would
// defeat that.
type provider interface {
	Complete(ctx context.Context, in LLMInput, defaultModel string, maxTokens int64) (LLMOutput, error)
	Name() string
}

// LLM executes model steps against whichever provider it was built with.
type LLM struct {
	provider     provider
	defaultModel string
	maxTokens    int64
}

// NewLLM selects a provider by name. Anything OpenAI-compatible — Moonshot/Kimi, Together, vLLM,
// Ollama, an OpenAI-compatible gateway — is the same provider with a different base URL.
func NewLLM(providerName, baseURL, apiKey, defaultModel string, maxTokens int64) (*LLM, error) {
	var p provider
	switch providerName {
	case "", "anthropic":
		var opts []option.RequestOption
		if apiKey != "" {
			opts = append(opts, option.WithAPIKey(apiKey))
		}
		p = &anthropicProvider{client: anthropic.NewClient(opts...)}
	case "openai-compatible":
		if baseURL == "" {
			return nil, fmt.Errorf("openai-compatible provider needs a base URL")
		}
		if apiKey == "" {
			return nil, fmt.Errorf("openai-compatible provider needs an API key")
		}
		p = &openAICompatProvider{
			baseURL: strings.TrimSuffix(baseURL, "/"),
			apiKey:  apiKey,
			http:    &http.Client{Timeout: 5 * time.Minute},
		}
	default:
		return nil, fmt.Errorf("unknown LLM provider %q (have anthropic, openai-compatible)", providerName)
	}
	return &LLM{provider: p, defaultModel: defaultModel, maxTokens: maxTokens}, nil
}

// Provider reports which provider is in use, for the startup log.
func (l *LLM) Provider() string { return l.provider.Name() }

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

	// No retry loop here on purpose. A model call that fails is a failed step, and a failed step is
	// already the control plane's problem to retry — adding a second retry policy inside the
	// executor would mean two components backing off against each other.
	out, err := l.provider.Complete(ctx, in, l.defaultModel, l.maxTokens)
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func (l *LLM) Close() error { return nil }

// --- Anthropic ---

type anthropicProvider struct {
	client anthropic.Client
}

func (a *anthropicProvider) Name() string { return "anthropic" }

func (a *anthropicProvider) Complete(ctx context.Context, in LLMInput, defaultModel string, maxTokens int64) (LLMOutput, error) {
	messages := make([]anthropic.MessageParam, 0, len(in.Messages))
	for i, m := range in.Messages {
		block := anthropic.NewTextBlock(m.Content)
		switch m.Role {
		case "user":
			messages = append(messages, anthropic.NewUserMessage(block))
		case "assistant":
			messages = append(messages, anthropic.NewAssistantMessage(block))
		default:
			return LLMOutput{}, fmt.Errorf("llm message %d has unknown role %q", i, m.Role)
		}
	}

	params := anthropic.MessageNewParams{
		Model:     anthropic.Model(firstNonEmpty(in.Model, defaultModel)),
		MaxTokens: firstNonZero(in.MaxTokens, maxTokens),
		Messages:  messages,
	}
	if in.System != "" {
		params.System = []anthropic.TextBlockParam{{Text: in.System}}
	}

	resp, err := a.client.Messages.New(ctx, params)
	if err != nil {
		return LLMOutput{}, fmt.Errorf("messages: %w", err)
	}

	var text string
	for _, block := range resp.Content {
		if t, ok := block.AsAny().(anthropic.TextBlock); ok {
			text += t.Text
		}
	}
	return LLMOutput{
		Text:         text,
		StopReason:   string(resp.StopReason),
		Model:        string(resp.Model),
		InputTokens:  resp.Usage.InputTokens,
		OutputTokens: resp.Usage.OutputTokens,
	}, nil
}

// --- OpenAI-compatible ---

// openAICompatProvider speaks the chat-completions shape that most non-Anthropic vendors expose.
//
// Written against net/http rather than a vendor SDK deliberately: the whole value of this provider
// is that one implementation serves every OpenAI-compatible endpoint, and pulling in a client
// library per vendor would give that up for nothing.
type openAICompatProvider struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func (o *openAICompatProvider) Name() string { return "openai-compatible(" + o.baseURL + ")" }

func (o *openAICompatProvider) Complete(ctx context.Context, in LLMInput, defaultModel string, maxTokens int64) (LLMOutput, error) {
	// The system prompt is a message with role "system" here, not a separate top-level field the
	// way Anthropic models it. That difference is the reason this interface exists.
	messages := make([]LLMMessage, 0, len(in.Messages)+1)
	if in.System != "" {
		messages = append(messages, LLMMessage{Role: "system", Content: in.System})
	}
	messages = append(messages, in.Messages...)

	body, err := json.Marshal(map[string]any{
		"model":      firstNonEmpty(in.Model, defaultModel),
		"max_tokens": firstNonZero(in.MaxTokens, maxTokens),
		"messages":   messages,
	})
	if err != nil {
		return LLMOutput{}, fmt.Errorf("encode chat request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return LLMOutput{}, fmt.Errorf("build chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.apiKey)

	resp, err := o.http.Do(req)
	if err != nil {
		return LLMOutput{}, fmt.Errorf("chat request: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return LLMOutput{}, fmt.Errorf("read chat response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// The body carries the vendor's error message and no credential, so it is safe and far more
		// useful than the status line alone.
		return LLMOutput{}, fmt.Errorf("chat completions returned %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	var decoded struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return LLMOutput{}, fmt.Errorf("decode chat response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return LLMOutput{}, fmt.Errorf("chat completions returned no choices")
	}

	return LLMOutput{
		Text:         decoded.Choices[0].Message.Content,
		StopReason:   decoded.Choices[0].FinishReason,
		Model:        decoded.Model,
		InputTokens:  decoded.Usage.PromptTokens,
		OutputTokens: decoded.Usage.CompletionTokens,
	}, nil
}

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
