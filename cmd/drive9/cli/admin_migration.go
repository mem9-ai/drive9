package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

// AdminMigration implements `drive9 admin migration`. It is the customer-facing
// control surface for the cross-cloud tenant migration; the controller and the
// data-plane agents are separate services.
func AdminMigration(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminMigrationUsage())
	}
	if isMigrationHelpArg(args[0]) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationUsage())
		return nil
	}
	switch args[0] {
	case "batch":
		return adminMigrationBatch(args[1:])
	case "start":
		return adminMigrationBatchAction(args[1:], "start")
	case "pause":
		return adminMigrationBatchAction(args[1:], "pause")
	case "resume":
		return adminMigrationBatchAction(args[1:], "resume")
	case "status":
		return adminMigrationStatus(args[1:])
	case "list":
		return adminMigrationList(args[1:])
	case "watch":
		return adminMigrationWatch(args[1:])
	case "rollback":
		return adminMigrationRollback(args[1:])
	case "state":
		return adminMigrationState(args[1:])
	default:
		return fmt.Errorf("unknown admin migration command %q\n%s", args[0], adminMigrationUsage())
	}
}

func adminMigrationBatch(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminMigrationBatchUsage())
	}
	if isMigrationHelpArg(args[0]) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationBatchUsage())
		return nil
	}
	switch args[0] {
	case "create":
		return adminMigrationBatchCreate(args[1:])
	case "get":
		return adminMigrationBatchGet(args[1:])
	default:
		return fmt.Errorf("unknown admin migration batch command %q\n%s", args[0], adminMigrationBatchUsage())
	}
}

type adminMigrationCommon struct {
	server       string
	regionCode   string
	publicKey    string
	privateKey   string
	asJSON       bool
	serverGiven  bool
	regionGiven  bool
	publicGiven  bool
	privateGiven bool
}

// parseAdminMigrationCommon pulls the flags shared by every migration command
// out of args and returns the command-specific remainder. It deliberately does
// not reject unknown flags: the caller's parser owns those and can produce a
// command-specific usage error.
func parseAdminMigrationCommon(args []string) (adminMigrationCommon, []string, error) {
	var common adminMigrationCommon
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--server":
			if i+1 >= len(args) {
				return common, nil, fmt.Errorf("--server requires an argument")
			}
			i++
			common.server = args[i]
			common.serverGiven = true
		case "--region-code":
			if i+1 >= len(args) {
				return common, nil, fmt.Errorf("--region-code requires an argument")
			}
			i++
			common.regionCode = args[i]
			common.regionGiven = true
		case "--tidbcloud-public-key":
			if i+1 >= len(args) {
				return common, nil, fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			i++
			common.publicKey = args[i]
			common.publicGiven = true
		case "--tidbcloud-private-key":
			if i+1 >= len(args) {
				return common, nil, fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			i++
			common.privateKey = args[i]
			common.privateGiven = true
		case "--json":
			common.asJSON = true
		default:
			rest = append(rest, args[i])
		}
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(common.server), common.serverGiven); err != nil {
		return common, nil, err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(common.regionCode), common.regionGiven); err != nil {
		return common, nil, err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(common.publicKey), common.publicGiven); err != nil {
		return common, nil, err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(common.privateKey), common.privateGiven); err != nil {
		return common, nil, err
	}
	return common, rest, nil
}

func (c adminMigrationCommon) target() (string, client.AdminMigrationCredentials, error) {
	r := ResolveCredentials()
	publicKey, privateKey := adminTiDBCloudKeys(c.publicKey, c.privateKey)
	if publicKey == "" || privateKey == "" {
		return "", client.AdminMigrationCredentials{}, fmt.Errorf("TiDB Cloud credentials are required; pass --tidbcloud-public-key and --tidbcloud-private-key or set %s/%s", EnvTiDBCloudPublicKey, EnvTiDBCloudPrivateKey)
	}
	server, err := quotaServer(c.server, c.regionCode, r.Server, true)
	if err != nil {
		return "", client.AdminMigrationCredentials{}, err
	}
	server = strings.TrimSpace(server)
	if server == "" {
		return "", client.AdminMigrationCredentials{}, fmt.Errorf("server URL is required; pass --server or set %s", EnvServer)
	}
	return server, client.AdminMigrationCredentials{PublicKey: publicKey, PrivateKey: privateKey}, nil
}

func isMigrationHelpArg(arg string) bool {
	switch arg {
	case "-h", "-help", "--help", "help":
		return true
	}
	return false
}

func migrationWantsHelp(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-h", "-help", "--help", "help":
			return true
		}
	}
	return false
}

func adminMigrationBatchCreate(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationBatchCreateUsage())
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	var (
		name      string
		file      string
		tenantIDs string
	)
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case "--name":
			if i+1 >= len(rest) {
				return fmt.Errorf("--name requires an argument")
			}
			i++
			name = strings.TrimSpace(rest[i])
		case "--file":
			if i+1 >= len(rest) {
				return fmt.Errorf("--file requires an argument")
			}
			i++
			file = strings.TrimSpace(rest[i])
		case "--tenant-ids":
			if i+1 >= len(rest) {
				return fmt.Errorf("--tenant-ids requires an argument")
			}
			i++
			tenantIDs = strings.TrimSpace(rest[i])
		default:
			return fmt.Errorf("unknown flag %q\n%s", rest[i], adminMigrationBatchCreateUsage())
		}
	}
	if (file == "") == (tenantIDs == "") {
		return fmt.Errorf("exactly one of --file or --tenant-ids is required")
	}
	var ids []string
	if file != "" {
		ids, err = readMigrationTenantFile(file)
		if err != nil {
			return err
		}
	} else {
		ids, err = parseMigrationTenantIDs(tenantIDs)
		if err != nil {
			return err
		}
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	raw, err := client.New(server, "").AdminCreateMigrationBatch(context.Background(), client.AdminMigrationBatchCreateRequest{
		Credentials: creds,
		Name:        name,
		TenantIDs:   ids,
	})
	if err != nil {
		return adminMigrationError("create migration batch", err)
	}
	if common.asJSON {
		return printAdminMigrationJSON(raw)
	}
	return printAdminMigrationBatch(raw)
}

func adminMigrationBatchGet(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationBatchGetUsage())
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	batchID, err := parseMigrationPositional(rest, adminMigrationBatchGetUsage(), "BATCH_ID")
	if err != nil {
		return err
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	raw, err := client.New(server, "").AdminGetMigrationBatch(context.Background(), client.AdminMigrationBatchRequest{
		Credentials: creds,
		BatchID:     batchID,
	})
	if err != nil {
		return adminMigrationError("get migration batch", err)
	}
	if common.asJSON {
		return printAdminMigrationJSON(raw)
	}
	return printAdminMigrationBatch(raw)
}

func adminMigrationBatchAction(args []string, action string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationActionUsage(action))
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	batchID, err := parseMigrationPositional(rest, adminMigrationActionUsage(action), "BATCH_ID")
	if err != nil {
		return err
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	c := client.New(server, "")
	req := client.AdminMigrationBatchRequest{Credentials: creds, BatchID: batchID}
	var raw json.RawMessage
	switch action {
	case "start":
		raw, err = c.AdminStartMigration(context.Background(), req)
	case "pause":
		raw, err = c.AdminPauseMigration(context.Background(), req)
	case "resume":
		raw, err = c.AdminResumeMigration(context.Background(), req)
	default:
		return fmt.Errorf("unsupported migration action %q", action)
	}
	if err != nil {
		return adminMigrationError(action+" migration batch", err)
	}
	if common.asJSON {
		return printAdminMigrationJSON(raw)
	}
	return printAdminMigrationAction(raw)
}

func adminMigrationStatus(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationStatusUsage())
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	tenantID, err := parseMigrationPositional(rest, adminMigrationStatusUsage(), "TENANT_ID")
	if err != nil {
		return err
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	raw, err := client.New(server, "").AdminGetMigrationTenant(context.Background(), client.AdminMigrationTenantRequest{
		Credentials: creds,
		TenantID:    tenantID,
	})
	if err != nil {
		return adminMigrationError("get migration tenant status", err)
	}
	if common.asJSON {
		return printAdminMigrationJSON(raw)
	}
	return printAdminMigrationTenant(raw)
}

func adminMigrationList(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationListUsage())
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	var req client.AdminMigrationTenantListRequest
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case "--batch":
			if i+1 >= len(rest) {
				return fmt.Errorf("--batch requires an argument")
			}
			i++
			req.BatchID = strings.TrimSpace(rest[i])
		case "--state":
			if i+1 >= len(rest) {
				return fmt.Errorf("--state requires an argument")
			}
			i++
			req.State = strings.TrimSpace(rest[i])
		case "--tenant-id":
			if i+1 >= len(rest) {
				return fmt.Errorf("--tenant-id requires an argument")
			}
			i++
			req.TenantID = strings.TrimSpace(rest[i])
		case "--tenant-ids":
			if i+1 >= len(rest) {
				return fmt.Errorf("--tenant-ids requires an argument")
			}
			i++
			ids, err := parseMigrationTenantIDs(rest[i])
			if err != nil {
				return err
			}
			req.TenantIDs = ids
		case "--page":
			if i+1 >= len(rest) {
				return fmt.Errorf("--page requires an argument")
			}
			i++
			req.Page, err = parseMigrationPositiveInt("--page", rest[i])
			if err != nil {
				return err
			}
		case "--page-size":
			if i+1 >= len(rest) {
				return fmt.Errorf("--page-size requires an argument")
			}
			i++
			req.PageSize, err = parseMigrationPositiveInt("--page-size", rest[i])
			if err != nil {
				return err
			}
		case "--limit":
			if i+1 >= len(rest) {
				return fmt.Errorf("--limit requires an argument")
			}
			i++
			req.Limit, err = parseMigrationPositiveInt("--limit", rest[i])
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown flag %q\n%s", rest[i], adminMigrationListUsage())
		}
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	req.Credentials = creds
	raw, err := client.New(server, "").AdminListMigrationTenants(context.Background(), req)
	if err != nil {
		return adminMigrationError("list migration tenants", err)
	}
	if common.asJSON {
		return printAdminMigrationJSON(raw)
	}
	return printAdminMigrationTenantList(raw)
}

func adminMigrationWatch(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationWatchUsage())
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	var req client.AdminMigrationWatchRequest
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case "--batch":
			if i+1 >= len(rest) {
				return fmt.Errorf("--batch requires an argument")
			}
			i++
			req.BatchID = strings.TrimSpace(rest[i])
		case "--tenant-id":
			if i+1 >= len(rest) {
				return fmt.Errorf("--tenant-id requires an argument")
			}
			i++
			req.TenantID = strings.TrimSpace(rest[i])
		case "--tenant-ids":
			if i+1 >= len(rest) {
				return fmt.Errorf("--tenant-ids requires an argument")
			}
			i++
			ids, err := parseMigrationTenantIDs(rest[i])
			if err != nil {
				return err
			}
			req.TenantIDs = ids
		default:
			return fmt.Errorf("unknown flag %q\n%s", rest[i], adminMigrationWatchUsage())
		}
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	req.Credentials = creds
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = client.New(server, "").WatchAdminMigrationEvents(ctx, req, func(event client.AdminMigrationEvent) error {
		return printAdminMigrationEvent(event, common.asJSON)
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return adminMigrationError("watch migration events", err)
	}
	return nil
}

func adminMigrationRollback(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationRollbackUsage())
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	tenantID, err := parseMigrationPositional(rest, adminMigrationRollbackUsage(), "TENANT_ID")
	if err != nil {
		return err
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	raw, err := client.New(server, "").AdminRollbackMigration(context.Background(), client.AdminMigrationRollbackRequest{
		Credentials: creds,
		TenantID:    tenantID,
	})
	if err != nil {
		return adminMigrationError("rollback migration tenant", err)
	}
	if common.asJSON {
		return printAdminMigrationJSON(raw)
	}
	return printAdminMigrationAction(raw)
}

func adminMigrationState(args []string) error {
	if migrationWantsHelp(args) {
		_, _ = fmt.Fprintln(os.Stdout, adminMigrationStateUsage())
		return nil
	}
	common, rest, err := parseAdminMigrationCommon(args)
	if err != nil {
		return err
	}
	tenantID, err := parseMigrationPositional(rest, adminMigrationStateUsage(), "TENANT_ID")
	if err != nil {
		return err
	}
	server, creds, err := common.target()
	if err != nil {
		return err
	}
	raw, err := client.New(server, "").AdminGetMigrationTenantState(context.Background(), client.AdminMigrationTenantRequest{
		Credentials: creds,
		TenantID:    tenantID,
	})
	if err != nil {
		return adminMigrationError("get migration tenant state", err)
	}
	if common.asJSON {
		return printAdminMigrationJSON(raw)
	}
	return printAdminMigrationState(raw)
}

func parseMigrationPositional(args []string, usage, name string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("%s is required\n%s", name, usage)
	}
	if len(args) > 1 {
		return "", fmt.Errorf("unexpected argument %q\n%s", args[1], usage)
	}
	value := strings.TrimSpace(args[0])
	if value == "" || strings.HasPrefix(value, "-") {
		return "", fmt.Errorf("%s is required\n%s", name, usage)
	}
	return value, nil
}

func parseMigrationPositiveInt(name, raw string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func parseMigrationTenantIDs(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	ids := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		id := strings.TrimSpace(part)
		if id == "" {
			return nil, fmt.Errorf("tenant id list contains an empty id")
		}
		if strings.ContainsAny(id, " \t\r\n/?#") {
			return nil, fmt.Errorf("tenant id %q contains an unsupported character", id)
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("tenant id %q appears more than once", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("at least one tenant id is required")
	}
	return ids, nil
}

func readMigrationTenantFile(path string) ([]string, error) {
	var (
		scanner *bufio.Scanner
		closeFn func() error
	)
	if path == "" {
		return nil, fmt.Errorf("tenant file path is required")
	}
	if path == "-" {
		scanner = bufio.NewScanner(os.Stdin)
		closeFn = func() error { return nil }
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open tenant file: %w", err)
		}
		scanner = bufio.NewScanner(file)
		closeFn = file.Close
	}
	defer func() { _ = closeFn() }()
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	var ids []string
	seen := make(map[string]struct{})
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, ",")
		for _, part := range parts {
			id := strings.TrimSpace(part)
			if id == "" {
				return nil, fmt.Errorf("tenant file contains an empty id")
			}
			if strings.ContainsAny(id, " \t\r\n/?#") {
				return nil, fmt.Errorf("tenant id %q contains an unsupported character", id)
			}
			if _, ok := seen[id]; ok {
				return nil, fmt.Errorf("tenant id %q appears more than once", id)
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read tenant file: %w", err)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("tenant file contains no tenant ids")
	}
	return ids, nil
}

func printAdminMigrationJSON(raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		_, err := fmt.Fprintln(os.Stdout, "{}")
		return err
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return fmt.Errorf("decode migration response: %w", err)
	}
	_, err := fmt.Fprintln(os.Stdout, out.String())
	return err
}

type adminMigrationBatchView struct {
	BatchID     string   `json:"batch_id"`
	Name        string   `json:"name"`
	State       string   `json:"state"`
	TenantCount int      `json:"tenant_count"`
	TenantIDs   []string `json:"tenant_ids"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
}

func printAdminMigrationBatch(raw json.RawMessage) error {
	var batch adminMigrationBatchView
	if err := json.Unmarshal(raw, &batch); err != nil {
		return fmt.Errorf("decode migration batch: %w", err)
	}
	if batch.BatchID == "" {
		return printAdminMigrationJSON(raw)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "BATCH_ID\tNAME\tSTATE\tTENANT_COUNT\tCREATED_AT")
	_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
		batch.BatchID,
		emptyAsDash(batch.Name),
		emptyAsDash(batch.State),
		batch.TenantCount,
		emptyAsDash(batch.CreatedAt),
	)
	return w.Flush()
}

type adminMigrationTenantView struct {
	TenantID         string         `json:"tenant_id"`
	BatchID          string         `json:"batch_id"`
	State            string         `json:"state"`
	SyncComplete     *bool          `json:"sync_complete"`
	BlockingReason   string         `json:"blocking_reason"`
	AMode            string         `json:"a_mode"`
	BMode            string         `json:"b_mode"`
	TargetEndpoint   string         `json:"target_endpoint"`
	TokenFingerprint string         `json:"token_fingerprint"`
	Message          string         `json:"message"`
	Error            string         `json:"error"`
	Progress         map[string]any `json:"progress"`
}

type adminMigrationTenantListView struct {
	Tenants  []adminMigrationTenantView `json:"tenants"`
	Page     int                        `json:"page"`
	PageSize int                        `json:"page_size"`
	NextPage int                        `json:"next_page"`
	Total    int                        `json:"total"`
}

func printAdminMigrationTenantList(raw json.RawMessage) error {
	var list adminMigrationTenantListView
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("decode migration tenant list: %w", err)
	}
	if list.Tenants == nil {
		return printAdminMigrationJSON(raw)
	}
	if len(list.Tenants) == 0 {
		_, err := fmt.Fprintln(os.Stdout, "no migration tenants")
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TENANT_ID\tBATCH_ID\tSTATE\tSYNC\tBLOCKING\tA_MODE\tB_MODE\tLAG_S\tROWS\tOBJECTS")
	for _, tenant := range list.Tenants {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			tenant.TenantID,
			emptyAsDash(tenant.BatchID),
			emptyAsDash(tenant.State),
			boolAsDash(tenant.SyncComplete),
			emptyAsDash(tenant.BlockingReason),
			emptyAsDash(tenant.AMode),
			emptyAsDash(tenant.BMode),
			progressValue(tenant.Progress, "lag_seconds"),
			progressValue(tenant.Progress, "rows_copied"),
			progressValue(tenant.Progress, "objects_copied"),
		)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if list.NextPage != 0 {
		_, err := fmt.Fprintf(os.Stdout, "next_page=%d page=%d page_size=%d\n", list.NextPage, list.Page, list.PageSize)
		return err
	}
	return nil
}

func printAdminMigrationTenant(raw json.RawMessage) error {
	var tenant adminMigrationTenantView
	if err := json.Unmarshal(raw, &tenant); err != nil {
		return fmt.Errorf("decode migration tenant: %w", err)
	}
	if tenant.TenantID == "" {
		return printAdminMigrationJSON(raw)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "FIELD\tVALUE")
	row := func(name, value string) { _, _ = fmt.Fprintf(w, "%s\t%s\n", name, emptyAsDash(value)) }
	row("tenant_id", tenant.TenantID)
	row("batch_id", tenant.BatchID)
	row("state", tenant.State)
	row("sync_complete", boolAsDash(tenant.SyncComplete))
	row("blocking_reason", tenant.BlockingReason)
	row("a_mode", tenant.AMode)
	row("b_mode", tenant.BMode)
	row("target_endpoint", tenant.TargetEndpoint)
	row("token_fingerprint", tenant.TokenFingerprint)
	row("lag_seconds", progressValue(tenant.Progress, "lag_seconds"))
	row("rows_copied", progressValue(tenant.Progress, "rows_copied"))
	row("objects_copied", progressValue(tenant.Progress, "objects_copied"))
	row("bytes_copied", progressValue(tenant.Progress, "bytes_copied"))
	if tenant.BlockingReason == "" {
		if tenant.Message != "" {
			row("message", tenant.Message)
		}
		if tenant.Error != "" {
			row("error", tenant.Error)
		}
	}
	return w.Flush()
}

func printAdminMigrationState(raw json.RawMessage) error {
	var state struct {
		TenantID        string `json:"tenant_id"`
		State           string `json:"state"`
		Epoch           int64  `json:"epoch"`
		QuotaReconciled bool   `json:"quota_reconciled"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return fmt.Errorf("decode migration state: %w", err)
	}
	if state.TenantID == "" {
		return printAdminMigrationJSON(raw)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TENANT_ID\tSTATE\tEPOCH\tQUOTA_RECONCILED")
	_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%t\n", state.TenantID, emptyAsDash(state.State), state.Epoch, state.QuotaReconciled)
	return w.Flush()
}

func printAdminMigrationAction(raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		_, err := fmt.Fprintln(os.Stdout, "ok")
		return err
	}
	var action map[string]any
	if err := json.Unmarshal(raw, &action); err != nil {
		return printAdminMigrationJSON(raw)
	}
	batchID, _ := action["batch_id"].(string)
	tenantID, _ := action["tenant_id"].(string)
	state, _ := action["state"].(string)
	message, _ := action["message"].(string)
	if batchID == "" && tenantID == "" && state == "" && message == "" {
		return printAdminMigrationJSON(raw)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "BATCH_ID\tTENANT_ID\tSTATE\tMESSAGE")
	_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", emptyAsDash(batchID), emptyAsDash(tenantID), emptyAsDash(state), emptyAsDash(message))
	return w.Flush()
}

func printAdminMigrationEvent(event client.AdminMigrationEvent, asJSON bool) error {
	if asJSON {
		payload := struct {
			Event string          `json:"event"`
			Data  json.RawMessage `json:"data,omitempty"`
		}{Event: event.Type, Data: event.Data}
		if len(event.Data) > 0 {
			payload.Data = event.Data
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(os.Stdout, string(raw))
		return err
	}
	var data map[string]any
	_ = json.Unmarshal(event.Data, &data)
	prefix := event.Type
	if prefix == "" {
		prefix = stringValue(data, "event", "type")
	}
	if prefix == "" {
		prefix = "event"
	}
	var fields []string
	for _, key := range []string{"tenant_id", "batch_id", "state", "blocking_reason", "message", "error"} {
		if value := stringValue(data, key); value != "" {
			fields = append(fields, key+"="+value)
		}
	}
	if len(fields) == 0 {
		if len(event.Data) > 0 {
			fields = append(fields, string(event.Data))
		}
	}
	_, err := fmt.Fprintf(os.Stdout, "[%s] %s\n", prefix, strings.Join(fields, " "))
	return err
}

func stringValue(data map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := data[key]
		if !ok {
			continue
		}
		switch v := value.(type) {
		case string:
			return v
		case json.Number:
			return v.String()
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return ""
}

func boolAsDash(value *bool) string {
	if value == nil {
		return "-"
	}
	if *value {
		return "true"
	}
	return "false"
}

func progressValue(progress map[string]any, key string) string {
	if progress == nil {
		return "-"
	}
	value, ok := progress[key]
	if !ok {
		return "-"
	}
	switch v := value.(type) {
	case string:
		if v == "" {
			return "-"
		}
		return v
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		return fmt.Sprint(v)
	}
}

// adminMigrationError preserves the controller's stable error code. Migration
// callers branch on codes such as MIGRATION_NOT_ENABLED, UMOUNT_REQUIRED and
// ROLLBACK_NOT_ALLOWED, so dropping the code would make the CLI unusable for
// automation.
func adminMigrationError(action string, err error) error {
	var statusErr *client.StatusError
	if errors.As(err, &statusErr) {
		message := strings.TrimSpace(statusErr.Message)
		if message == "" {
			message = http.StatusText(statusErr.StatusCode)
		}
		if message == "" {
			message = fmt.Sprintf("HTTP %d", statusErr.StatusCode)
		}
		if code := strings.TrimSpace(statusErr.Code); code != "" {
			return fmt.Errorf("%s failed (%s): %s", action, code, message)
		}
		return fmt.Errorf("%s failed (HTTP %d): %s", action, statusErr.StatusCode, message)
	}
	return fmt.Errorf("%s failed: %w", action, err)
}

func adminMigrationUsage() string {
	return `usage: drive9 admin migration <command> [arguments]

commands:
  batch create [--name NAME] (--file FILE | --tenant-ids IDS)
                                   register a migration batch
  batch get BATCH_ID               show one batch
  start BATCH_ID                   start a prepared batch
  pause BATCH_ID                   pause dispatch for a batch
  resume BATCH_ID                  resume a paused batch
  status TENANT_ID                 show one tenant's migration status
  list [flags]                     list migration tenants
  watch [flags]                    stream migration events
  rollback TENANT_ID               roll back a switched tenant before target writes
  state TENANT_ID                  show the local read-only fence state

global migration flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output the controller response as JSON

examples:
  drive9 admin migration batch create --file tenants.list
  drive9 admin migration start b_xxx
  drive9 admin migration status tnt_xxx --json
  drive9 admin migration list --batch b_xxx --state switched
  drive9 admin migration watch --batch b_xxx
  drive9 admin migration rollback tnt_xxx`
}

func adminMigrationBatchUsage() string {
	return `usage: drive9 admin migration batch <command> [arguments]

commands:
  create [--name NAME] (--file FILE | --tenant-ids IDS)
                                   register a migration batch
  get BATCH_ID                     show one batch`
}

func adminMigrationBatchCreateUsage() string {
	return `usage: drive9 admin migration batch create [--name NAME] (--file FILE | --tenant-ids IDS)

flags:
  --name NAME                      optional human-readable batch name
  --file FILE                      file with one tenant id per line, or "-" for stdin
  --tenant-ids IDS                 comma-separated tenant ids`
}

func adminMigrationBatchGetUsage() string {
	return `usage: drive9 admin migration batch get BATCH_ID`
}

func adminMigrationActionUsage(action string) string {
	return fmt.Sprintf("usage: drive9 admin migration %s BATCH_ID", action)
}

func adminMigrationStatusUsage() string {
	return `usage: drive9 admin migration status TENANT_ID`
}

func adminMigrationListUsage() string {
	return `usage: drive9 admin migration list [flags]

flags:
  --batch ID                       filter by batch
  --state STATE                    filter by state
  --tenant-id ID                   filter by one tenant
  --tenant-ids IDS                 filter by comma-separated tenant ids
  --page N                         page number
  --page-size N                    page size
  --limit N                        result limit`
}

func adminMigrationWatchUsage() string {
	return `usage: drive9 admin migration watch [flags]

flags:
  --batch ID                       watch one batch
  --tenant-id ID                   watch one tenant
  --tenant-ids IDS                 watch comma-separated tenant ids`
}

func adminMigrationRollbackUsage() string {
	return `usage: drive9 admin migration rollback TENANT_ID`
}

func adminMigrationStateUsage() string {
	return `usage: drive9 admin migration state TENANT_ID`
}
