package objectfs

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	_ "github.com/rclone/rclone/backend/azureblob"          // register azureblob
	_ "github.com/rclone/rclone/backend/googlecloudstorage" // register gcs
	_ "github.com/rclone/rclone/backend/local"              // VFS disk cache
	_ "github.com/rclone/rclone/backend/s3"                 // register s3
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
)

var installOnce sync.Once

func ensureRclone() {
	installOnce.Do(func() {
		configfile.Install()
		ci := fs.GetConfig(context.Background())
		ci.LogLevel = fs.LogLevelError
		// Process-wide: also affects later pkg/s3client uploads in this process.
		// AWS SDK v2 defaults to adding checksums whenever supported.
		// Trailer checksums need TLS, so streamed (unseekable) PutObject
		// to http:// MinIO/local endpoints fails. WHEN_REQUIRED keeps
		// HTTPS AWS behavior and unblocks HTTP.
		if os.Getenv("AWS_REQUEST_CHECKSUM_CALCULATION") == "" {
			_ = os.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_REQUIRED")
		}
		if os.Getenv("AWS_RESPONSE_CHECKSUM_VALIDATION") == "" {
			_ = os.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "WHEN_REQUIRED")
		}
	})
}

// OpenFs creates an rclone Fs rooted at loc.Bucket/loc.Path (the mount prefix).
// If loc points at a single object, err may be nil and fileLeaf is set.
func OpenFs(ctx context.Context, loc Location) (f fs.Fs, fileLeaf string, err error) {
	ensureRclone()
	spec, err := connectionString(loc)
	if err != nil {
		return nil, "", err
	}
	f, err = fs.NewFs(ctx, spec)
	if err == fs.ErrorIsFile {
		return f, path.Base(strings.TrimSuffix(loc.Path, "/")), nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("open %s: %w", loc.Raw, err)
	}
	return f, "", nil
}

// OpenFsBucket creates an rclone Fs rooted at loc.Bucket only.
func OpenFsBucket(ctx context.Context, loc Location) (fs.Fs, error) {
	root := loc
	root.Path = ""
	root.DirHint = true
	f, _, err := OpenFs(ctx, root)
	return f, err
}

func objectRoot(loc Location) string {
	root := loc.Bucket
	if p := strings.Trim(loc.Path, "/"); p != "" {
		root += "/" + p
	}
	return root
}

func connectionString(loc Location) (string, error) {
	if loc.Bucket == "" {
		return "", fmt.Errorf("objectfs: empty bucket in %s", loc.Raw)
	}
	switch CanonicalScheme(loc.Scheme) {
	case SchemeGS:
		return gcsConnectionString(loc)
	case SchemeAZ:
		return azureConnectionString(loc)
	default:
		return s3ConnectionString(loc)
	}
}

func gcsConnectionString(loc Location) (string, error) {
	params := []string{"env_auth=true"}
	if ep := loc.Query[QueryEndpoint]; ep != "" {
		params = append(params, "endpoint="+quote(ep))
	}
	return ":gcs," + strings.Join(params, ",") + ":" + objectRoot(loc), nil
}

func azureConnectionString(loc Location) (string, error) {
	params := []string{"env_auth=true"}
	if acct := loc.Query[QueryAccount]; acct != "" {
		params = append(params, "account="+quote(acct))
	}
	if ep := loc.Query[QueryEndpoint]; ep != "" {
		params = append(params, "endpoint="+quote(ep))
	}
	return ":azureblob," + strings.Join(params, ",") + ":" + objectRoot(loc), nil
}

func s3ConnectionString(loc Location) (string, error) {
	provider := "AWS"
	switch loc.Scheme {
	case SchemeCOS:
		provider = "TencentCOS"
	case SchemeOSS:
		provider = "Alibaba"
	case SchemeTOS:
		provider = "Other"
	}
	if v := loc.Query[QueryProvider]; v != "" {
		switch v {
		case "tencent":
			provider = "TencentCOS"
		case "aliyun":
			provider = "Alibaba"
		case "volcengine":
			provider = "Other"
		case "aws":
			provider = "AWS"
		}
	}

	params := []string{
		"provider=" + provider,
		"env_auth=true",
		"directory_markers=true",
	}
	region := loc.Query[QueryRegion]
	if region != "" {
		params = append(params, "region="+quote(region))
	}
	endpoint := loc.Query[QueryEndpoint]
	if endpoint == "" && loc.Scheme == SchemeTOS {
		if region == "" {
			return "", fmt.Errorf("objectfs: region required for %s (set ?region=)", loc.Raw)
		}
		endpoint = "https://tos-s3-" + region + ".volces.com"
	}
	if endpoint != "" {
		params = append(params, "endpoint="+quote(endpoint))
	}
	if loc.Query[QueryForcePathStyle] == "true" {
		params = append(params, "force_path_style=true")
	}
	// HTTP endpoints cannot use AWS SDK trailer checksums (needs TLS).
	if strings.HasPrefix(endpoint, "http://") {
		params = append(params, "use_unsigned_payload=true")
	}
	return ":s3," + strings.Join(params, ",") + ":" + objectRoot(loc), nil
}

func quote(s string) string {
	if s == "" {
		return `""`
	}
	if !strings.ContainsAny(s, `,:="`) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
