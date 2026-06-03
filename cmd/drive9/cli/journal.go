package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/journal"
)

type repeatStrings []string

func (r *repeatStrings) String() string {
	return strings.Join(*r, ",")
}

func (r *repeatStrings) Set(value string) error {
	*r = append(*r, value)
	return nil
}

func Journal(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", journalUsage())
	}
	if IsHelpArg(args[0]) {
		_, _ = fmt.Fprintln(os.Stdout, journalUsage())
		return nil
	}
	switch args[0] {
	case "new":
		return JournalNew(args[1:])
	case "append":
		return JournalAppend(args[1:])
	case "cat":
		return JournalCat(args[1:])
	case "find":
		return JournalFind(args[1:])
	case "verify":
		return JournalVerify(args[1:])
	case "seal":
		return fmt.Errorf("journal seal is not implemented in the Phase 1 journal backend")
	default:
		return fmt.Errorf("unknown journal command %q", args[0])
	}
}

func journalUsage() string {
	return "usage: drive9 journal <new|append|cat|find|verify>"
}

func journalNewUsage() string { return "usage: drive9 journal new [flags]" }

func journalAppendUsage() string { return "usage: drive9 journal append <journal> [flags]" }

func journalCatUsage() string { return "usage: drive9 journal cat <journal> [flags]" }

func journalFindUsage() string { return "usage: drive9 journal find [flags]" }

func journalVerifyUsage() string { return "usage: drive9 journal verify <journal> [--json]" }

func JournalNew(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, journalNewUsage())
		return nil
	}
	fs := flag.NewFlagSet("journal new", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var meta repeatStrings
	journalID := fs.String("id", "", "journal id")
	kind := fs.String("kind", journal.DefaultKind, "journal kind")
	kindShort := fs.String("k", journal.DefaultKind, "journal kind")
	title := fs.String("title", "", "journal title")
	asJSON := fs.Bool("json", false, "print JSON")
	fs.Var(&meta, "meta", "metadata key=value")
	fs.Var(&meta, "m", "metadata key=value")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printJournalHelp(fs, "usage: drive9 journal new [flags]")
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%s", journalNewUsage())
	}
	resolvedKind := *kind
	if *kindShort != journal.DefaultKind {
		resolvedKind = *kindShort
	}
	if *journalID == "" {
		*journalID = journal.NewID("jrn")
	}
	req := journal.CreateRequest{
		JournalID: *journalID,
		Kind:      resolvedKind,
		Title:     *title,
	}
	parsedMeta, err := parseJournalAssignments(meta)
	if err != nil {
		return err
	}
	req.Labels = parsedMeta
	c := NewFromEnv()
	created, err := c.CreateJournal(context.Background(), req)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(created)
	}
	_, _ = fmt.Fprintln(os.Stdout, created.JournalID)
	return nil
}

func JournalAppend(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, journalAppendUsage())
		return nil
	}
	fs := flag.NewFlagSet("journal append", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var subjects repeatStrings
	defaultType := fs.String("type", "", "default entry type")
	defaultTypeShort := fs.String("t", "", "default entry type")
	source := fs.String("source", "", "entry source")
	appendID := fs.String("idempotency-key", "", "append id")
	jsonArray := fs.Bool("json-array", false, "read JSON array")
	fs.Var(&subjects, "subject", "subject")
	fs.Var(&subjects, "s", "subject")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printJournalHelp(fs,
				"usage: drive9 journal append <journal> [flags]",
				"",
				"Read journal entries from stdin.",
				"Default format is JSONL (one JSON object per line).",
				"Use --json-array to read a JSON array instead.",
				"",
				"Each entry needs a 'type' field either in the input or via --type/-t.",
				"",
				"Example:",
				`  echo '{"type":"task.started","summary":{"msg":"hello"}}' | drive9 journal append my-journal`,
				"",
			)
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%s", journalAppendUsage())
	}
	resolvedType := *defaultType
	if *defaultTypeShort != "" {
		resolvedType = *defaultTypeShort
	}
	entries, err := readJournalEntriesFromStdin(os.Stdin, *jsonArray)
	if err != nil {
		return err
	}
	applyDefaultJournalEntryTypes(entries, resolvedType)
	if err := validateJournalEntryTypes(entries, *jsonArray); err != nil {
		return err
	}
	for i := range entries {
		if *source != "" {
			entries[i].Source = *source
		}
		entries[i].Subjects = append(append([]string{}, subjects...), entries[i].Subjects...)
	}
	if *appendID == "" {
		*appendID = journal.NewID("app")
	}
	c := NewFromEnv()
	resp, err := c.AppendJournalEntries(context.Background(), fs.Arg(0), *appendID, entries)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	return enc.Encode(resp)
}

func readJournalEntriesFromStdin(r io.Reader, jsonArray bool) ([]journal.EntryInput, error) {
	if jsonArray {
		var entries []journal.EntryInput
		if err := json.NewDecoder(r).Decode(&entries); err != nil {
			return nil, err
		}
		return entries, nil
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	var entries []journal.EntryInput
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry journal.EntryInput
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("decode JSONL at line %d: %w", lineNum, err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no journal entries on stdin")
	}
	return entries, nil
}

func JournalCat(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, journalCatUsage())
		return nil
	}
	fs := flag.NewFlagSet("journal cat", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	after := fs.Int64("after", 0, "start after sequence")
	limit := fs.Int("limit", journal.DefaultLimit, "limit")
	follow := fs.Bool("f", false, "follow")
	fs.BoolVar(follow, "follow", false, "follow")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printJournalHelp(fs, "usage: drive9 journal cat <journal> [flags]")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%s", journalCatUsage())
	}
	c := NewFromEnv()
	journalID := fs.Arg(0)
	enc := json.NewEncoder(os.Stdout)
	for {
		entries, err := c.ReadJournalEntries(context.Background(), journalID, *after, *limit)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := enc.Encode(entry); err != nil {
				return err
			}
			*after = entry.Seq
		}
		if !*follow {
			return nil
		}
		time.Sleep(time.Second)
	}
}

func JournalFind(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, journalFindUsage())
		return nil
	}
	fs := flag.NewFlagSet("journal find", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var subjects repeatStrings
	var meta repeatStrings
	entryType := fs.String("type", "", "entry type")
	entryTypeShort := fs.String("t", "", "entry type")
	kind := fs.String("kind", "", "journal kind")
	actor := fs.String("actor", "", "actor type:id")
	status := fs.String("status", "", "status")
	since := fs.String("since", "", "since duration or RFC3339 time")
	until := fs.String("until", "", "until RFC3339 time")
	limit := fs.Int("limit", journal.DefaultLimit, "limit")
	cursor := fs.String("cursor", "", "cursor; repeat original filters")
	entries := fs.Bool("entries", false, "emit full entries")
	asJSON := fs.Bool("json", false, "emit JSONL")
	fs.Var(&subjects, "subject", "subject")
	fs.Var(&subjects, "s", "subject")
	fs.Var(&meta, "meta", "metadata key=value")
	fs.Var(&meta, "m", "metadata key=value")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printJournalHelp(fs, "usage: drive9 journal find [flags]")
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%s", journalFindUsage())
	}
	resolvedType := *entryType
	if *entryTypeShort != "" {
		resolvedType = *entryTypeShort
	}
	req := journal.SearchRequest{
		Type:     resolvedType,
		Kind:     *kind,
		Status:   *status,
		Subjects: subjects,
		Limit:    *limit,
		Cursor:   *cursor,
		Entries:  *entries,
	}
	parsedMeta, err := parseJournalAssignments(meta)
	if err != nil {
		return err
	}
	req.Labels = parsedMeta
	if *actor != "" {
		actorType, actorID, err := journal.SplitActor(*actor)
		if err != nil {
			return err
		}
		req.ActorType, req.ActorID = actorType, actorID
	}
	if *since != "" {
		t, err := parseJournalCLITimeOrDuration(*since)
		if err != nil {
			return err
		}
		req.Since = &t
		req.SinceRaw = *since
	}
	if *until != "" {
		t, err := time.Parse(time.RFC3339Nano, *until)
		if err != nil {
			return err
		}
		t = journal.NormalizeTime(t)
		req.Until = &t
		req.UntilRaw = *until
	}
	c := NewFromEnv()
	matches, err := c.SearchJournal(context.Background(), req)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	for _, match := range matches {
		if *entries {
			if match.Entry == nil {
				continue
			}
			if err := enc.Encode(match.Entry); err != nil {
				return err
			}
			continue
		}
		if *asJSON {
			if err := enc.Encode(match); err != nil {
				return err
			}
			continue
		}
		if match.Seq > 0 {
			_, _ = fmt.Fprintf(os.Stdout, "%s\t%d\t%s\t%s\n", match.JournalID, match.Seq, match.Type, journal.FormatTime(match.ObservedAt))
		} else {
			_, _ = fmt.Fprintf(os.Stdout, "%s\t%s\t%s\n", match.JournalID, match.Kind, journal.FormatTime(match.CreatedAt))
		}
	}
	return nil
}

func JournalVerify(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, journalVerifyUsage())
		return nil
	}
	fs := flag.NewFlagSet("journal verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printJournalHelp(fs, "usage: drive9 journal verify <journal> [flags]")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%s", journalVerifyUsage())
	}
	c := NewFromEnv()
	result, err := c.VerifyJournal(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(result)
	}
	status := "failed"
	if result.OK {
		status = "ok"
	}
	_, _ = fmt.Fprintf(os.Stdout, "%s journal=%s entries=%d head=%s\n", status, result.JournalID, result.Entries, result.HeadHash)
	if result.OK {
		return nil
	}
	return fmt.Errorf("journal verification failed")
}

func parseJournalAssignments(values []string) ([]journal.Label, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]journal.Label, 0, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("invalid metadata %q (expected key=value)", raw)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("invalid metadata %q (empty key)", raw)
		}
		out = append(out, journal.Label{Key: key, Value: strings.TrimSpace(value)})
	}
	if len(out) == 0 {
		return nil, nil
	}
	return journal.NormalizeLabels(out), nil
}

func printJournalHelp(fs *flag.FlagSet, lines ...string) error {
	for _, line := range lines {
		_, _ = fmt.Fprintln(os.Stderr, line)
	}
	_, _ = fmt.Fprintln(os.Stderr, "flags:")
	fs.SetOutput(os.Stderr)
	fs.PrintDefaults()
	return nil
}

func applyDefaultJournalEntryTypes(entries []journal.EntryInput, defaultType string) {
	for i := range entries {
		if entries[i].Type == "" {
			entries[i].Type = defaultType
		}
	}
}

func validateJournalEntryTypes(entries []journal.EntryInput, jsonArray bool) error {
	for i := range entries {
		if entries[i].Type == "" {
			if jsonArray {
				return fmt.Errorf("journal entry %d missing required 'type' field; provide --type/-t <type> or include \"type\":\"...\" in each JSON array item", i+1)
			}
			return fmt.Errorf("journal entry %d missing required 'type' field; provide --type/-t <type> or include \"type\":\"...\" in each JSONL line", i+1)
		}
	}
	return nil
}

// isBoolFlag reports whether the named flag in fs is a boolean flag.
func isBoolFlag(fs *flag.FlagSet, name string) bool {
	f := fs.Lookup(name)
	if f == nil {
		return false
	}
	_, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok
}

// normalizeHelpFlags converts standalone "-h" tokens to "-help" so that
// Go's flag package prints usage instead of erroring.  It skips conversion
// when "-h" is a value for a non-bool flag (determined dynamically from fs)
// or when it appears after "--".
func normalizeHelpFlags(args []string, fs *flag.FlagSet) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if a == "--" {
			copy(out[i:], args[i:])
			return out
		}
		if i > 0 && a == "-h" {
			prev := strings.TrimLeft(args[i-1], "-")
			if f := fs.Lookup(prev); f != nil && !isBoolFlag(fs, prev) {
				// -h is a value for a non-bool flag, don't rewrite.
				out[i] = a
				continue
			}
		}
		if a == "-h" {
			out[i] = "-help"
		} else {
			out[i] = a
		}
	}
	return out
}

func parseJournalCLITimeOrDuration(raw string) (time.Time, error) {
	if d, err := time.ParseDuration(raw); err == nil {
		return journal.NormalizeTime(time.Now().Add(-d)), nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		if unix, convErr := strconv.ParseInt(raw, 10, 64); convErr == nil {
			return journal.NormalizeTime(time.Unix(unix, 0)), nil
		}
		return time.Time{}, err
	}
	return journal.NormalizeTime(t), nil
}
