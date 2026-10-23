package fs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
)

// Chain is an ordered dispatch pipeline. There is no privileged core: the
// last handler is the terminal, and any handler that calls next past the
// end receives ErrNoCore.
type Chain struct {
	outs  []Handler
	armed atomic.Bool
}

// NewChain assembles handlers outermost-first. Use appends after the
// current tail, making the appended handler the new terminal.
func NewChain(handlers ...Handler) (*Chain, error) {
	c := &Chain{}
	if err := c.Use(handlers...); err != nil {
		return nil, err
	}
	return c, nil
}

// Use appends handlers after the current tail. It rejects nil (including
// typed-nil) handlers, and fails once the chain has started dispatching —
// composition is frozen at first Dispatch.
func (c *Chain) Use(handlers ...Handler) error {
	if c == nil {
		return fmt.Errorf("fs: nil chain")
	}
	if c.armed.Load() {
		return fmt.Errorf("fs: chain already dispatching")
	}
	for _, h := range handlers {
		if h == nil || isNilHandler(h) {
			return fmt.Errorf("fs: nil handler")
		}
	}
	c.outs = append(c.outs, handlers...)
	return nil
}

// Empty reports whether the chain has no handlers.
func (c *Chain) Empty() bool {
	return c == nil || len(c.outs) == 0
}

// Handlers returns the chain's handlers outermost-first.
func (c *Chain) Handlers() []Handler {
	if c == nil {
		return nil
	}
	return append([]Handler(nil), c.outs...)
}

// Dispatch runs op through the chain starting at the outermost handler.
func (c *Chain) Dispatch(ctx context.Context, op *Op) (Result, error) {
	// SetAttr mode presence compat: direct chain dispatchers that predate
	// HasMode set Mode without the flag. A non-zero Mode implies presence;
	// the facade always sets HasMode explicitly so chmod(000) (Mode == 0)
	// still distinguishes present-vs-absent.
	if op.Kind == OpSetAttr && !op.HasMode && op.Mode != 0 {
		op.HasMode = true
	}
	if c == nil {
		return Result{}, fmt.Errorf("fs: nil chain")
	}
	c.armed.Store(true)
	return c.dispatchFrom(0, ctx, op)
}

func (c *Chain) dispatchFrom(i int, ctx context.Context, op *Op) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if i >= len(c.outs) {
		return Result{}, ErrNoCore
	}
	next := func(nctx context.Context, nop *Op) (Result, error) {
		return c.dispatchFrom(i+1, nctx, nop)
	}
	return c.outs[i].Do(ctx, op, next)
}

func (c *Chain) handlers() []Handler {
	if c == nil {
		return nil
	}
	return c.outs
}

// Lifecycle is implemented by handlers that own background state (upload
// workers, watchers, sweepers). The chain is the registry: assembly
// type-asserts each handler once and fails fast when a required
// implementation is missing.
type Lifecycle interface {
	Start(ctx context.Context) error
	Drain(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Start starts lifecycle handlers in chain order (outermost first).
func (c *Chain) Start(ctx context.Context) error {
	var errs []error
	for _, h := range c.handlers() {
		if l, ok := h.(Lifecycle); ok {
			if err := l.Start(ctx); err != nil {
				errs = append(errs, fmt.Errorf("fs: start: %w", err))
			}
		}
	}
	return errors.Join(errs...)
}

// Drain quiesces background work in reverse chain order (innermost
// terminal first, then outward) so every layer observes its dependencies
// settled before it settles itself.
func (c *Chain) Drain(ctx context.Context) error {
	hs := c.handlers()
	var errs []error
	for i := len(hs) - 1; i >= 0; i-- {
		if l, ok := hs[i].(Lifecycle); ok {
			if err := l.Drain(ctx); err != nil {
				errs = append(errs, fmt.Errorf("fs: drain: %w", err))
			}
		}
	}
	return errors.Join(errs...)
}

// Stop stops lifecycle handlers in reverse chain order, after Drain.
func (c *Chain) Stop(ctx context.Context) error {
	hs := c.handlers()
	var errs []error
	for i := len(hs) - 1; i >= 0; i-- {
		if l, ok := hs[i].(Lifecycle); ok {
			if err := l.Stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("fs: stop: %w", err))
			}
		}
	}
	return errors.Join(errs...)
}

func isNilHandler(h Handler) bool {
	v := reflect.ValueOf(h)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	}
	return false
}
