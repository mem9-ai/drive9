package server

import (
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
)

// testDBInfo holds parsed connection parameters from testDSN for tests that
// need to build tenant DSNs or create branch-provisioner fakes. Use
// newTestDBInfo to obtain an instance.
type testDBInfo struct {
	Meta   *meta.Store
	Pool   *tenant.Pool
	DBHost string
	DBPort int
	DBUser string
	DBPass string
	DBName string
}

// newTestDBInfo opens a shared meta store + pool backed by testDSN and
// registers cleanup. Callers get deterministic host/port/user/pass without
// duplicating DSN-parsing logic.
func newTestDBInfo(t *testing.T) *testDBInfo {
	t.Helper()
	if testDSN == "" {
		t.Skip("no test database available")
	}
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })

	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host, port := "127.0.0.1", 3306
	if parsed.Addr != "" {
		h, p, _ := strings.Cut(parsed.Addr, ":")
		if h != "" {
			host = h
		}
		if p != "" {
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost", SkipTiDBSchemaCheck: true}, enc)
	pool.SetMetaStore(metaStore)
	t.Cleanup(pool.Close)
	return &testDBInfo{Meta: metaStore, Pool: pool, DBHost: host, DBPort: port, DBUser: parsed.User, DBPass: parsed.Passwd, DBName: parsed.DBName}
}
