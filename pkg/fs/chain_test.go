package fs

import (
	"context"
	"errors"
	"testing"
)

type fake struct {
	name    string
	rewrite func(*Op)
	answer  *Result
	err     error
	calls   *[]string
	res     *Result
}

func (f *fake) Do(ctx context.Context, op *Op, next Next) (Result, error) {
	if f.calls != nil {
		*f.calls = append(*f.calls, f.name)
	}
	if f.rewrite != nil {
		f.rewrite(op)
	}
	if f.err != nil {
		return Result{}, f.err
	}
	if f.res != nil {
		if f.calls != nil {
			*f.calls = append(*f.calls, "core("+op.Path+")")
		}
		return *f.res, nil
	}
	if f.answer != nil {
		return *f.answer, nil
	}
	return next(ctx, op)
}

func coreHandler(res Result, calls ...*[]string) Handler {
	r := res
	f := &fake{name: "core", res: &r}
	if len(calls) > 0 {
		f.calls = calls[0]
	}
	return f
}

func TestChainAssemblyValidation(t *testing.T) {
	a := &fake{name: "a"}
	b := &fake{name: "b"}

	if _, err := NewChain(nil); err == nil {
		t.Fatal("nil core accepted")
	}
	var typedNil *fake
	if _, err := NewChain(typedNil); err == nil {
		t.Fatal("typed-nil core accepted")
	}
	ch, err := NewChain(coreHandler(Result{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Use(a, nil); err == nil {
		t.Fatal("nil outer accepted")
	}
	if err := ch.Use(typedNil); err == nil {
		t.Fatal("typed-nil outer accepted")
	}
	if err := ch.Use(a, b); err != nil {
		t.Fatalf("two-ext chain rejected: %v", err)
	}
}

func TestChainUseFrozenAfterDispatch(t *testing.T) {
	ch, err := NewChain(coreHandler(Result{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(context.Background(), &Op{Kind: OpGetAttr, Path: "/a"}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Use(&fake{name: "late"}); err == nil {
		t.Fatal("Use after dispatch accepted")
	}
}

func TestChainCoreCallingNextIsError(t *testing.T) {
	ch, err := NewChain(HandlerFunc(func(ctx context.Context, op *Op, next Next) (Result, error) {
		return next(ctx, op)
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = ch.Dispatch(context.Background(), &Op{Kind: OpGetAttr, Path: "/a"})
	if err == nil || !errors.Is(err, ErrNoCore) {
		t.Fatalf("core next misuse not caught: %v", err)
	}
}

func TestEmptyChainDispatchIsNoCore(t *testing.T) {
	ch, err := NewChain()
	if err != nil {
		t.Fatal(err)
	}
	if !ch.Empty() {
		t.Fatal("fresh chain not empty")
	}
	if _, err := ch.Dispatch(context.Background(), &Op{Kind: OpGetAttr, Path: "/a"}); !errors.Is(err, ErrNoCore) {
		t.Fatalf("empty chain dispatch = %v, want ErrNoCore", err)
	}
	ch2, err := NewChain(coreHandler(Result{}))
	if err != nil {
		t.Fatal(err)
	}
	if ch2.Empty() {
		t.Fatal("chain with one handler still empty")
	}
}

func TestChainDenyPropagates(t *testing.T) {
	var seen []string
	gate := &fake{name: "gate", err: ErrNotFound, calls: &seen}
	ch, err := NewChain(gate, coreHandler(Result{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = ch.Dispatch(context.Background(), &Op{Kind: OpRead, Path: "/g"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("denial did not propagate: %v", err)
	}
	if len(seen) != 1 || seen[0] != "gate" {
		t.Fatalf("flow = %v, want only the gate", seen)
	}
}

func TestChainRewriteAndShortCircuit(t *testing.T) {
	var seen []string
	ns := &fake{name: "ns", rewrite: func(op *Op) { op.Path = "/slot" + op.Path }, calls: &seen}
	ch, err := NewChain(ns, coreHandler(Result{}, &seen))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(context.Background(), &Op{Kind: OpGetAttr, Path: "/a"}); err != nil {
		t.Fatal(err)
	}
	if len(seen) == 0 || seen[len(seen)-1] != "core(/slot/a)" {
		t.Fatalf("core saw %v, want /slot/a", seen)
	}

	seen = nil
	attr := Attr{Size: 7}
	loc := &fake{name: "loc", answer: &Result{Attr: &attr}, calls: &seen}
	ch2, err := NewChain(loc, coreHandler(Result{}, &seen))
	if err != nil {
		t.Fatal(err)
	}
	res, err := ch2.Dispatch(context.Background(), &Op{Kind: OpGetAttr, Path: "/x"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 7 {
		t.Fatalf("short-circuit answer lost: %+v", res)
	}
	if len(seen) != 1 || seen[0] != "loc" {
		t.Fatalf("flow = %v, want only the answering layer", seen)
	}
}

type lifeHandler struct {
	fake
	log *[]string
}

func (l *lifeHandler) Start(ctx context.Context) error {
	*l.log = append(*l.log, "start:"+l.name)
	return nil
}

func (l *lifeHandler) Drain(ctx context.Context) error {
	*l.log = append(*l.log, "drain:"+l.name)
	return nil
}

func (l *lifeHandler) Stop(ctx context.Context) error {
	*l.log = append(*l.log, "stop:"+l.name)
	return nil
}

func TestChainLifecycle(t *testing.T) {
	var log []string
	core := &lifeHandler{fake: fake{name: "core"}, log: &log}
	plain := &fake{name: "plain"}
	a := &lifeHandler{fake: fake{name: "a"}, log: &log}
	b := &lifeHandler{fake: fake{name: "b"}, log: &log}

	// Flat chain: a and b wrap the plain passthrough, core is the terminal.
	ch, err := NewChain(a, plain, b, core)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ch.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ch.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ch.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"start:a", "start:b", "start:core",
		"drain:core", "drain:b", "drain:a",
		"stop:core", "stop:b", "stop:a",
	}
	if len(log) != len(want) {
		t.Fatalf("log = %v, want %v", log, want)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("log = %v, want %v", log, want)
		}
	}
}
