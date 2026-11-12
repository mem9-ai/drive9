package tidbcloudnative

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"go.uber.org/zap"
)

const (
	EnvTiDBCloudLocalRuntime   = "DRIVE9_TIDBCLOUD_LOCAL_RUNTIME" // docker|podman
	EnvTiDBCloudLocalTiDBImage = "DRIVE9_TIDBCLOUD_LOCAL_TIDB_IMAGE"
	EnvTiDBCloudLocalHost      = "DRIVE9_TIDBCLOUD_LOCAL_HOST" // host clients use to reach containers
	// EnvTiDBCloudLocalSessionID tags every tenant TiDB container with
	// label drive9.local.session=<id> so harness scripts can clean only this run.
	EnvTiDBCloudLocalSessionID = "DRIVE9_TIDBCLOUD_LOCAL_SESSION_ID"

	// EnvTiDBCloudLocalPublicKey and EnvTiDBCloudLocalPrivateKey pin the
	// server-owned key pair the local control plane authenticates. They must be
	// set together. When both are unset a random pair is generated per process,
	// so the backend never accepts a credential that ships in the source tree.
	EnvTiDBCloudLocalPublicKey  = "DRIVE9_TIDBCLOUD_LOCAL_PUBLIC_KEY"
	EnvTiDBCloudLocalPrivateKey = "DRIVE9_TIDBCLOUD_LOCAL_PRIVATE_KEY"

	defaultLocalTiDBImage = "pingcap/tidb:v8.5.6"
	localUserPrefix       = "local"
	localSessionLabelKey  = "drive9.local.session"

	// localCredentialKeyPrefix and localCredentialBytes shape a generated local
	// control-plane key pair: 16 random bytes per half, hex encoded.
	localCredentialKeyPrefix = "drive9-local-"
	localCredentialBytes     = 16

	// Concurrent warm-pool grow + real-time create can race on host
	// ports. Serialize allocate+run and retry on bind conflicts.
	localCreatePortAttempts = 8
	localPickPortAttempts   = 32
	localCreateMySQLWait    = 90 * time.Second
)

// LocalClustersAPI simulates TiDB Cloud OpenAPI with one Docker/Podman TiDB
// instance per cluster identity. Labels, spending limits, and org binding are
// tracked in-process so drive9 warm-pool control paths run
// unchanged. Each Cloud cluster is one container (multi-pool => multi-TiDB).
type LocalClustersAPI struct {
	runtime   string
	image     string
	orgID     string
	host      string
	sessionID string

	// credential is the only key pair authorize accepts. It is pinned by the
	// local key env vars or generated at startup; it is never a constant.
	credential localCredential

	// createMu serializes host-port allocation + container start so concurrent
	// Create/BatchCreate (warm grow vs real-time pool) do not pick the same port.
	createMu sync.Mutex

	mu            sync.Mutex
	clusters      map[string]*localCluster // clusterID -> state
	branches      map[string]*localBranch  // clusterID/branchID -> state
	reservedPorts map[int]struct{}         // ports held between docker start and registry insert
}

// localCredential is the key pair the local control plane authenticates. The
// local backend is the only issuer and the only verifier of it.
type localCredential struct {
	publicKey  string
	privateKey string
}

type localCluster struct {
	info          clusterInfo
	rootPassword  string
	containerName string
	containerID   string
}

type localBranch struct {
	info         branchInfo
	clusterID    string
	rootPassword string
}

type LocalClustersAPIConfig struct {
	Runtime   string
	Image     string
	Host      string
	SessionID string
	// PublicKey and PrivateKey pin the server-owned control-plane credential.
	// Leaving both empty generates a random pair for this process.
	PublicKey  string
	PrivateKey string
}

func NewLocalClustersAPIFromEnv() (*LocalClustersAPI, error) {
	runtime := strings.TrimSpace(os.Getenv(EnvTiDBCloudLocalRuntime))
	if runtime == "" {
		if _, err := exec.LookPath("docker"); err == nil {
			runtime = "docker"
		} else if _, err := exec.LookPath("podman"); err == nil {
			runtime = "podman"
		} else {
			return nil, fmt.Errorf("%s is unset and neither docker nor podman is on PATH", EnvTiDBCloudLocalRuntime)
		}
	}
	if _, err := exec.LookPath(runtime); err != nil {
		return nil, fmt.Errorf("local clusters runtime %q not found: %w", runtime, err)
	}
	image := strings.TrimSpace(os.Getenv(EnvTiDBCloudLocalTiDBImage))
	if image == "" {
		image = defaultLocalTiDBImage
	}
	host := strings.TrimSpace(os.Getenv(EnvTiDBCloudLocalHost))
	if host == "" {
		host = "127.0.0.1"
	}
	publicKey := strings.TrimSpace(os.Getenv(EnvTiDBCloudLocalPublicKey))
	privateKey := strings.TrimSpace(os.Getenv(EnvTiDBCloudLocalPrivateKey))
	api, err := NewLocalClustersAPI(LocalClustersAPIConfig{
		Runtime:    runtime,
		Image:      image,
		Host:       host,
		SessionID:  strings.TrimSpace(os.Getenv(EnvTiDBCloudLocalSessionID)),
		PublicKey:  publicKey,
		PrivateKey: privateKey,
	})
	if err != nil {
		return nil, err
	}
	if publicKey == "" && privateKey == "" {
		// An unpinned deployment only knows its credential in memory, so external
		// control-plane callers cannot authenticate. Say so once at startup
		// instead of letting them collect 401s; the values are never logged.
		logger.Info(context.Background(), "local_clusters_credential_generated",
			zap.String("hint", "set "+EnvTiDBCloudLocalPublicKey+" and "+EnvTiDBCloudLocalPrivateKey+" to pin a known local control-plane key pair"))
	}
	return api, nil
}

func NewLocalClustersAPI(cfg LocalClustersAPIConfig) (*LocalClustersAPI, error) {
	credential, err := resolveLocalCredential(cfg.PublicKey, cfg.PrivateKey)
	if err != nil {
		return nil, err
	}
	return &LocalClustersAPI{
		runtime:       cfg.Runtime,
		image:         cfg.Image,
		orgID:         localOrgID,
		host:          cfg.Host,
		sessionID:     strings.TrimSpace(cfg.SessionID),
		credential:    credential,
		clusters:      map[string]*localCluster{},
		branches:      map[string]*localBranch{},
		reservedPorts: map[int]struct{}{},
	}, nil
}

// Credentials returns the server-owned key pair this local control plane
// accepts. Internal callers (the provisioner) use it to execute cluster
// operations; it is never derived from the source tree.
func (a *LocalClustersAPI) Credentials() (publicKey, privateKey string) {
	if a == nil {
		return "", ""
	}
	return a.credential.publicKey, a.credential.privateKey
}

// resolveLocalCredential returns the only pair the local control plane accepts.
// A pinned pair must be complete; otherwise a fresh random pair is generated so
// the backend never falls back to a compiled-in credential.
func resolveLocalCredential(publicKey, privateKey string) (localCredential, error) {
	publicKey = strings.TrimSpace(publicKey)
	privateKey = strings.TrimSpace(privateKey)
	if publicKey == "" && privateKey == "" {
		return generateLocalCredential()
	}
	if publicKey == "" || privateKey == "" {
		return localCredential{}, fmt.Errorf("%s and %s must be set together", EnvTiDBCloudLocalPublicKey, EnvTiDBCloudLocalPrivateKey)
	}
	// Reject values no caller could ever present: a control character cannot be
	// carried in an X-TiDBCloud-*-Key header, so accepting one at startup would
	// produce a healthy server that 401s every external caller.
	if err := validateLocalCredentialEnvValue(EnvTiDBCloudLocalPublicKey, publicKey); err != nil {
		return localCredential{}, err
	}
	if err := validateLocalCredentialEnvValue(EnvTiDBCloudLocalPrivateKey, privateKey); err != nil {
		return localCredential{}, err
	}
	return localCredential{publicKey: publicKey, privateKey: privateKey}, nil
}

// validateLocalCredentialEnvValue rejects the ASCII control characters (C0 and
// DEL) that HTTP header values cannot carry.
func validateLocalCredentialEnvValue(envVar, value string) error {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s contains a control character that cannot be sent in a request header", envVar)
		}
	}
	return nil
}

func generateLocalCredential() (localCredential, error) {
	buf := make([]byte, 2*localCredentialBytes)
	if _, err := rand.Read(buf); err != nil {
		return localCredential{}, fmt.Errorf("generate local control-plane credential: %w", err)
	}
	return localCredential{
		publicKey:  localCredentialKeyPrefix + "pk-" + hex.EncodeToString(buf[:localCredentialBytes]),
		privateKey: localCredentialKeyPrefix + "sk-" + hex.EncodeToString(buf[localCredentialBytes:]),
	}, nil
}

var _ ClustersAPI = (*LocalClustersAPI)(nil)

func (a *LocalClustersAPI) CreateCluster(ctx context.Context, publicKey, privateKey string, body []byte) (*clusterInfo, error) {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return nil, err
	}
	req, err := parseLocalCreateBody(body)
	if err != nil {
		return nil, err
	}
	return a.createOne(ctx, req)
}

func (a *LocalClustersAPI) BatchCreateClusters(ctx context.Context, publicKey, privateKey string, body []byte) ([]clusterInfo, error) {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return nil, err
	}
	var envelope struct {
		Requests []struct {
			Cluster json.RawMessage `json:"cluster"`
		} `json:"requests"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode local batch create: %w", err)
	}
	if len(envelope.Requests) == 0 {
		return nil, nil
	}
	out := make([]clusterInfo, 0, len(envelope.Requests))
	var partial error
	for i, item := range envelope.Requests {
		req, err := parseLocalCreateBody(item.Cluster)
		if err != nil {
			partial = fmt.Errorf("batch item %d: %w", i, err)
			break
		}
		info, err := a.createOne(ctx, req)
		if err != nil {
			partial = fmt.Errorf("batch item %d: %w", i, err)
			break
		}
		out = append(out, *info)
	}
	return out, partial
}

func (a *LocalClustersAPI) GetCluster(ctx context.Context, publicKey, privateKey, clusterID string) (*clusterInfo, error) {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	lc, ok := a.clusters[strings.TrimSpace(clusterID)]
	if !ok {
		return nil, newAPIStatusError("cluster get", http.StatusNotFound, "")
	}
	return cloneClusterInfo(&lc.info), nil
}

func (a *LocalClustersAPI) ListClusters(ctx context.Context, publicKey, privateKey string, query url.Values) ([]clusterInfo, string, error) {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return nil, "", err
	}
	_ = ctx
	filter := ""
	if query != nil {
		filter = query.Get("filter")
	}
	wantIDs := parseClusterIDFilter(filter)
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]clusterInfo, 0, len(a.clusters))
	for id, lc := range a.clusters {
		if len(wantIDs) > 0 {
			if _, ok := wantIDs[id]; !ok {
				continue
			}
		}
		if strings.Contains(filter, Drive9ManagedLabel) && lc.info.Labels[Drive9ManagedLabel] != "true" {
			continue
		}
		out = append(out, *cloneClusterInfo(&lc.info))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClusterID < out[j].ClusterID })
	return out, "", nil
}

func (a *LocalClustersAPI) PatchCluster(ctx context.Context, publicKey, privateKey, clusterID string, body []byte) error {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return err
	}
	var patch struct {
		UpdateMask string `json:"updateMask"`
		Cluster    struct {
			Labels        map[string]string `json:"labels"`
			SpendingLimit *struct {
				Monthly int32 `json:"monthly"`
			} `json:"spendingLimit"`
		} `json:"cluster"`
	}
	if err := json.Unmarshal(body, &patch); err != nil {
		return fmt.Errorf("decode local cluster patch: %w", err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	lc, ok := a.clusters[strings.TrimSpace(clusterID)]
	if !ok {
		return newAPIStatusError("cluster patch", http.StatusNotFound, "")
	}
	mask := strings.TrimSpace(patch.UpdateMask)
	if mask == "labels" || strings.Contains(mask, "labels") {
		if lc.info.Labels == nil {
			lc.info.Labels = map[string]string{}
		}
		for k, v := range patch.Cluster.Labels {
			lc.info.Labels[k] = v
		}
		lc.info.Labels[TiDBCloudOrganizationLabel] = a.orgID
	}
	if mask == "spendingLimit.monthly" || strings.Contains(mask, "spendingLimit") {
		if patch.Cluster.SpendingLimit != nil {
			lc.info.SpendingLimit = &struct {
				Monthly int32 `json:"monthly"`
			}{Monthly: patch.Cluster.SpendingLimit.Monthly}
		}
	}
	return nil
}

func (a *LocalClustersAPI) DeleteCluster(ctx context.Context, publicKey, privateKey, clusterID string) error {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return err
	}
	clusterID = strings.TrimSpace(clusterID)
	a.mu.Lock()
	lc, ok := a.clusters[clusterID]
	if ok {
		delete(a.clusters, clusterID)
		if p := lc.info.Endpoints.Public.Port; p > 0 {
			delete(a.reservedPorts, p)
		}
		for key, br := range a.branches {
			if br.clusterID == clusterID {
				delete(a.branches, key)
			}
		}
	}
	a.mu.Unlock()
	if !ok {
		return nil
	}
	target := lc.containerID
	if target == "" {
		target = lc.containerName
	}
	if target != "" {
		_ = a.runtimeCmd(ctx, "rm", "-f", target).Run()
	}
	return nil
}

func (a *LocalClustersAPI) CreateBranch(ctx context.Context, publicKey, privateKey, clusterID string, body []byte) (*branchInfo, error) {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return nil, err
	}
	req, err := parseLocalCreateBody(body)
	if err != nil {
		var br struct {
			DisplayName  string `json:"displayName"`
			RootPassword string `json:"rootPassword"`
		}
		if err2 := json.Unmarshal(body, &br); err2 != nil {
			return nil, err
		}
		req = localCreateRequest{DisplayName: br.DisplayName, RootPassword: br.RootPassword, Labels: map[string]string{}}
	}
	a.mu.Lock()
	_, ok := a.clusters[strings.TrimSpace(clusterID)]
	a.mu.Unlock()
	if !ok {
		return nil, newAPIStatusError("branch provision", http.StatusNotFound, "parent cluster not found")
	}
	info, err := a.createOne(ctx, req)
	if err != nil {
		return nil, err
	}
	branchID := "branch-" + info.ClusterID
	bi := branchInfo{
		BranchID:   branchID,
		State:      stateActive,
		UserPrefix: info.UserPrefix,
	}
	bi.Endpoints.Public.Host = info.Endpoints.Public.Host
	bi.Endpoints.Public.Port = info.Endpoints.Public.Port
	bi.Endpoints.Private.Host = info.Endpoints.Private.Host
	bi.Endpoints.Private.Port = info.Endpoints.Private.Port
	a.mu.Lock()
	a.branches[clusterID+"/"+branchID] = &localBranch{info: bi, clusterID: clusterID, rootPassword: req.RootPassword}
	a.mu.Unlock()
	return &bi, nil
}

func (a *LocalClustersAPI) GetBranch(ctx context.Context, publicKey, privateKey, clusterID, branchID string) (*branchInfo, error) {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	br, ok := a.branches[strings.TrimSpace(clusterID)+"/"+strings.TrimSpace(branchID)]
	if !ok {
		return nil, newAPIStatusError("branch get", http.StatusNotFound, "")
	}
	out := br.info
	return &out, nil
}

func (a *LocalClustersAPI) DeleteBranch(ctx context.Context, publicKey, privateKey, clusterID, branchID string) error {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return err
	}
	key := strings.TrimSpace(clusterID) + "/" + strings.TrimSpace(branchID)
	a.mu.Lock()
	_, ok := a.branches[key]
	if ok {
		delete(a.branches, key)
	}
	a.mu.Unlock()
	if !ok {
		return nil
	}
	if strings.HasPrefix(branchID, "branch-") {
		cid := strings.TrimPrefix(branchID, "branch-")
		_ = a.DeleteCluster(ctx, publicKey, privateKey, cid)
	}
	return nil
}

func (a *LocalClustersAPI) ResolveAPIKey(ctx context.Context, publicKey, privateKey string) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	if err := a.authorize(publicKey, privateKey); err != nil {
		return nil, err
	}
	_ = ctx
	return &tenant.TiDBCloudAPIKeyIdentity{
		OrganizationID: a.orgID,
		Role:           tenant.TiDBCloudRoleOrgOwner,
	}, nil
}

// Close removes all containers started by this API (best-effort).
func (a *LocalClustersAPI) Close(ctx context.Context) {
	a.mu.Lock()
	ids := make([]string, 0, len(a.clusters))
	for _, lc := range a.clusters {
		if lc.containerID != "" {
			ids = append(ids, lc.containerID)
		} else if lc.containerName != "" {
			ids = append(ids, lc.containerName)
		}
	}
	a.clusters = map[string]*localCluster{}
	a.branches = map[string]*localBranch{}
	a.reservedPorts = map[int]struct{}{}
	a.mu.Unlock()
	for _, id := range ids {
		_ = a.runtimeCmd(ctx, "rm", "-f", id).Run()
	}
}

type localCreateRequest struct {
	DisplayName  string
	RootPassword string
	Labels       map[string]string
	Spending     *int32
}

func parseLocalCreateBody(body []byte) (localCreateRequest, error) {
	var raw struct {
		DisplayName   string            `json:"displayName"`
		RootPassword  string            `json:"rootPassword"`
		Labels        map[string]string `json:"labels"`
		SpendingLimit *struct {
			Monthly int32 `json:"monthly"`
		} `json:"spendingLimit"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return localCreateRequest{}, fmt.Errorf("decode local create cluster: %w", err)
	}
	req := localCreateRequest{
		DisplayName:  strings.TrimSpace(raw.DisplayName),
		RootPassword: strings.TrimSpace(raw.RootPassword),
		Labels:       raw.Labels,
	}
	if req.Labels == nil {
		req.Labels = map[string]string{}
	}
	if raw.SpendingLimit != nil {
		v := raw.SpendingLimit.Monthly
		req.Spending = &v
	}
	if req.RootPassword == "" {
		return localCreateRequest{}, fmt.Errorf("rootPassword is required")
	}
	return req, nil
}

func (a *LocalClustersAPI) createOne(ctx context.Context, req localCreateRequest) (*clusterInfo, error) {
	clusterID := "local-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	containerName := "drive9-tidb-" + clusterID

	var (
		port        int
		containerID string
		lastStart   error
	)
	for attempt := 1; attempt <= localCreatePortAttempts; attempt++ {
		startedPort, startedID, err := a.startContainerOnFreePort(ctx, containerName)
		if err == nil {
			port = startedPort
			containerID = startedID
			lastStart = nil
			break
		}
		lastStart = err
		if !isPortConflictError(err.Error()) || attempt == localCreatePortAttempts {
			return nil, err
		}
		logger.Warn(ctx, "local_clusters_port_conflict_retry",
			zap.String("cluster_id", clusterID),
			zap.String("container", containerName),
			zap.Int("attempt", attempt),
			zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	if lastStart != nil {
		return nil, lastStart
	}

	// Wait/bootstrap outside createMu so other pools can start concurrently.
	if err := a.waitMySQL(ctx, a.host, port, "", localCreateMySQLWait); err != nil {
		a.releaseReservedPort(port)
		_ = a.runtimeCmd(context.Background(), "rm", "-f", containerName).Run()
		return nil, fmt.Errorf("wait local tidb ready: %w", err)
	}
	if err := a.bootstrapUsers(ctx, a.host, port, req.RootPassword); err != nil {
		a.releaseReservedPort(port)
		_ = a.runtimeCmd(context.Background(), "rm", "-f", containerName).Run()
		return nil, fmt.Errorf("bootstrap local tidb users: %w", err)
	}

	labels := map[string]string{}
	for k, v := range req.Labels {
		labels[k] = v
	}
	labels[Drive9ManagedLabel] = "true"
	labels[TiDBCloudOrganizationLabel] = a.orgID

	info := clusterInfo{
		ClusterID:  clusterID,
		State:      stateActive,
		Labels:     labels,
		UserPrefix: localUserPrefix,
	}
	info.Endpoints.Public.Host = a.host
	info.Endpoints.Public.Port = port
	info.Endpoints.Private.Host = a.host
	info.Endpoints.Private.Port = port
	if req.Spending != nil {
		info.SpendingLimit = &struct {
			Monthly int32 `json:"monthly"`
		}{Monthly: *req.Spending}
	}

	a.mu.Lock()
	a.clusters[clusterID] = &localCluster{
		info:          info,
		rootPassword:  req.RootPassword,
		containerName: containerName,
		containerID:   containerID,
	}
	delete(a.reservedPorts, port)
	a.mu.Unlock()

	logger.Info(ctx, "local_clusters_create_ready",
		zap.String("cluster_id", clusterID),
		zap.String("host", a.host),
		zap.Int("port", port))
	return cloneClusterInfo(&info), nil
}

// startContainerOnFreePort picks a host port not already used by this API,
// starts the TiDB container, and marks the port reserved until registry insert
// or failure cleanup. createMu covers allocate+run only.
func (a *LocalClustersAPI) startContainerOnFreePort(ctx context.Context, containerName string) (port int, containerID string, err error) {
	a.createMu.Lock()
	defer a.createMu.Unlock()

	port, err = a.allocateHostPort()
	if err != nil {
		return 0, "", err
	}
	logger.Info(ctx, "local_clusters_create_start",
		zap.String("container", containerName),
		zap.Int("port", port),
		zap.String("image", a.image),
		zap.String("host", a.host))

	args := []string{
		"run", "-d", "--rm",
		"--name", containerName,
		"-p", fmt.Sprintf("%s:%d:4000", a.host, port),
	}
	if a.sessionID != "" {
		args = append(args, "--label", localSessionLabelKey+"="+a.sessionID)
	}
	args = append(args, a.image)
	cmd := a.runtimeCmd(ctx, args...)
	out, runErr := cmd.CombinedOutput()
	msg := strings.TrimSpace(string(out))
	if runErr != nil {
		// Port may have been claimed between Listen close and docker bind.
		return 0, "", fmt.Errorf("start local tidb container on %s:%d: %w: %s", a.host, port, runErr, msg)
	}
	containerID = strings.TrimSpace(msg)
	if containerID == "" {
		return 0, "", fmt.Errorf("start local tidb container returned empty id")
	}
	a.reservePort(port)
	return port, containerID, nil
}

func (a *LocalClustersAPI) allocateHostPort() (int, error) {
	used := a.snapshotUsedHostPorts()
	for try := 0; try < localPickPortAttempts; try++ {
		port, err := pickLocalPort()
		if err != nil {
			return 0, err
		}
		if _, taken := used[port]; taken {
			continue
		}
		// Avoid immediately reusing a port we just closed if the kernel still
		// lists it; mark candidate used for this allocation scan.
		used[port] = struct{}{}
		return port, nil
	}
	return 0, fmt.Errorf("could not allocate a free host port after %d attempts", localPickPortAttempts)
}

func (a *LocalClustersAPI) snapshotUsedHostPorts() map[int]struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[int]struct{}, len(a.clusters)+len(a.reservedPorts))
	for _, c := range a.clusters {
		if c == nil {
			continue
		}
		if p := c.info.Endpoints.Public.Port; p > 0 {
			out[p] = struct{}{}
		}
	}
	for p := range a.reservedPorts {
		out[p] = struct{}{}
	}
	return out
}

func (a *LocalClustersAPI) reservePort(port int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.reservedPorts == nil {
		a.reservedPorts = map[int]struct{}{}
	}
	a.reservedPorts[port] = struct{}{}
}

func (a *LocalClustersAPI) releaseReservedPort(port int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.reservedPorts, port)
}

func isPortConflictError(msg string) bool {
	msg = strings.ToLower(msg)
	// docker / podman / libpod common phrases when host port is taken
	for _, needle := range []string{
		"port is already allocated",
		"address already in use",
		"bind: address already in use",
		"failed to bind host port",
		"port is already open",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	// e.g. "cannot listen on the TCP port: listen tcp4 :4001: bind: address already in use"
	if strings.Contains(msg, "port") && strings.Contains(msg, "already") {
		return true
	}
	return false
}

func (a *LocalClustersAPI) bootstrapUsers(ctx context.Context, host string, port int, password string) error {
	cfg := mysql.NewConfig()
	cfg.User = "root"
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	user := systemRootUsernameFor(localUserPrefix)
	stmts := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s", quoteString(user), quoteString(password)),
		fmt.Sprintf("ALTER USER %s IDENTIFIED BY %s", quoteString(user), quoteString(password)),
		fmt.Sprintf("GRANT ALL ON *.* TO %s WITH GRANT OPTION", quoteString(user)),
		fmt.Sprintf("ALTER USER %s IDENTIFIED BY %s", quoteString("root"), quoteString(password)),
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func (a *LocalClustersAPI) waitMySQL(ctx context.Context, host string, port int, password string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		cfg := mysql.NewConfig()
		cfg.User = "root"
		cfg.Passwd = password
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%d", host, port)
		cfg.Timeout = 2 * time.Second
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err == nil {
			err = db.PingContext(ctx)
			_ = db.Close()
		}
		if err == nil {
			return nil
		}
		last = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for mysql at %s:%d: %w", host, port, last)
}

// authorize verifies the caller-supplied key pair against the server-owned
// local credential. LocalClustersAPI simulates the TiDB Cloud control plane, so
// it keeps the same invariant as the real API: only the configured pair is
// authenticated, and that pair is what ResolveAPIKey maps to the org-owner
// identity. Anything else fails closed with 401 instead of being trusted.
func (a *LocalClustersAPI) authorize(publicKey, privateKey string) error {
	publicKey = strings.TrimSpace(publicKey)
	privateKey = strings.TrimSpace(privateKey)
	if publicKey == "" || privateKey == "" {
		return tenant.ErrCredentialsRequired
	}
	// Compare both halves without short-circuiting so a matching public key is
	// not distinguishable from a matching private key through timing.
	publicMatches := subtle.ConstantTimeCompare([]byte(publicKey), []byte(a.credential.publicKey))
	privateMatches := subtle.ConstantTimeCompare([]byte(privateKey), []byte(a.credential.privateKey))
	if publicMatches&privateMatches != 1 {
		return newAPIStatusError("api key authorize", http.StatusUnauthorized, "")
	}
	return nil
}

func (a *LocalClustersAPI) runtimeCmd(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, a.runtime, args...)
}

func pickLocalPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = ln.Close() }()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listen addr %T", ln.Addr())
	}
	return addr.Port, nil
}

func cloneClusterInfo(in *clusterInfo) *clusterInfo {
	if in == nil {
		return nil
	}
	out := *in
	if in.Labels != nil {
		out.Labels = make(map[string]string, len(in.Labels))
		for k, v := range in.Labels {
			out.Labels[k] = v
		}
	}
	if in.SpendingLimit != nil {
		s := *in.SpendingLimit
		out.SpendingLimit = &s
	}
	return &out
}

func parseClusterIDFilter(filter string) map[string]struct{} {
	out := map[string]struct{}{}
	const prefix = "clusterId ="
	idx := strings.Index(filter, prefix)
	if idx < 0 {
		return out
	}
	rest := strings.TrimSpace(filter[idx+len(prefix):])
	if !strings.HasPrefix(rest, `"`) {
		return out
	}
	rest = rest[1:]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return out
	}
	for _, id := range strings.Split(rest[:end], ",") {
		id = strings.TrimSpace(id)
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}
