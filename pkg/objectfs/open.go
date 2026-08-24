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

// SessionCredentials are short-lived STS keys minted by drive9-server.
type SessionCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	SASURL          string
	AccessToken     string
}

// OpenFs creates an rclone Fs rooted at loc.Bucket/loc.Path (the mount prefix).
// If loc points at a single object, err may be nil and fileLeaf is set.
func OpenFs(ctx context.Context, loc Location) (f fs.Fs, fileLeaf string, err error) {
	return OpenFsWithSession(ctx, loc, SessionCredentials{})
}

func OpenFsWithSession(ctx context.Context, loc Location, sess SessionCredentials) (f fs.Fs, fileLeaf string, err error) {
	ensureRclone()
	spec, err := connectionString(loc, sess)
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
	f, _, err := OpenFsWithSession(ctx, root, SessionCredentials{})
	return f, err
}

func OpenFsBucketWithSession(ctx context.Context, loc Location, sess SessionCredentials) (fs.Fs, error) {
	root := loc
	root.Path = ""
	root.DirHint = true
	f, _, err := OpenFsWithSession(ctx, root, sess)
	return f, err
}

func objectRoot(loc Location) string {
	root := loc.Bucket
	if p := strings.Trim(loc.Path, "/"); p != "" {
		root += "/" + p
	}
	return root
}

func connectionString(loc Location, sess SessionCredentials) (string, error) {
	if loc.Bucket == "" {
		return "", fmt.Errorf("objectfs: empty bucket in %s", loc.Raw)
	}
	switch CanonicalScheme(loc.Scheme) {
	case SchemeGS:
		return gcsConnectionString(loc, sess)
	case SchemeAZ:
		return azureConnectionString(loc, sess)
	default:
		return s3ConnectionString(loc, sess)
	}
}

func gcsConnectionString(loc Location, sess SessionCredentials) (string, error) {
	var params []string
	if sess.AccessToken != "" {
		params = append(params, "env_auth=false", "access_token="+quote(sess.AccessToken))
	} else {
		params = append(params, "env_auth=true")
	}
	if ep := loc.Query[QueryEndpoint]; ep != "" {
		params = append(params, "endpoint="+quote(ep))
	}
	return ":gcs," + strings.Join(params, ",") + ":" + objectRoot(loc), nil
}

func azureConnectionString(loc Location, sess SessionCredentials) (string, error) {
	var params []string
	if sess.SASURL != "" {
		params = append(params, "env_auth=false", "sas_url="+quote(sess.SASURL))
	} else {
		params = append(params, "env_auth=true")
	}
	if acct := loc.Query[QueryAccount]; acct != "" {
		params = append(params, "account="+quote(acct))
	}
	if ep := loc.Query[QueryEndpoint]; ep != "" {
		params = append(params, "endpoint="+quote(ep))
	}
	return ":azureblob," + strings.Join(params, ",") + ":" + objectRoot(loc), nil
}

func s3ConnectionString(loc Location, sess SessionCredentials) (string, error) {
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
	}
	if sess.AccessKeyID != "" {
		params = append(params, "env_auth=false", "directory_markers=true", "access_key_id="+quote(sess.AccessKeyID), "secret_access_key="+quote(sess.SecretAccessKey))
		if sess.SessionToken != "" {
			params = append(params, "session_token="+quote(sess.SessionToken))
		}
	} else {
		params = append(params, "env_auth=true", "directory_markers=true")
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
