package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Router sends each step to the executor registered for its kind.
//
// A run mixes kinds — the agent asks the model what to do, then asks for a command to be run — and
// those go to different places: one to the Claude API, one into a sandbox. The control plane still
// sees a single Executor, so nothing upstream learns that steps are handled by more than one thing.
type Router struct {
	byKind map[string]Executor
}

func NewRouter(byKind map[string]Executor) *Router {
	return &Router{byKind: byKind}
}

func (r *Router) Execute(ctx context.Context, req Request) (json.RawMessage, error) {
	target, ok := r.byKind[req.Kind]
	if !ok {
		return nil, fmt.Errorf("no executor for step kind %q (have %s)", req.Kind, strings.Join(r.kinds(), ", "))
	}
	return target.Execute(ctx, req)
}

// Close closes every registered executor, and reports all failures rather than the first — one
// executor refusing to shut down should not hide another leaking.
func (r *Router) Close() error {
	var errs []error
	for _, kind := range r.kinds() {
		if err := r.byKind[kind].Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s executor: %w", kind, err))
		}
	}
	return errors.Join(errs...)
}

func (r *Router) kinds() []string {
	kinds := make([]string, 0, len(r.byKind))
	for kind := range r.byKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}
