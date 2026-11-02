// Command drive9-mock-server runs the embedded drive9 backend used by the
// simulator: the drive9-server-api-spec.md wire contract over SQLite and a
// directory-backed S3-compatible endpoint. See pkg/mockserver and
// simulator/ROADMAP.md (G2).
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/mockserver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9009", "listen address")
	dir := flag.String("dir", "", "state directory (meta.db, tenant-*.db, objects/); required")
	inline := flag.Int64("inline-threshold", 1<<30, "advertised inline cutoff (Tier 0 accepts every PUT inline)")
	maxUpload := flag.Int64("max-upload-bytes", 1<<32, "max upload total_size")
	s3Endpoint := flag.String("s3-endpoint", "", "external S3 endpoint advertised by /v1/data-credential (e.g. embedded rustfs); empty = built-in file scheme")
	s3Bucket := flag.String("s3-bucket", "drive9-mock", "S3 bucket advertised with --s3-endpoint")
	s3AK := flag.String("s3-ak", "drive9mock", "S3 access key advertised with --s3-endpoint")
	s3SK := flag.String("s3-sk", "drive9mock", "S3 secret key advertised with --s3-endpoint")
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "--dir is required")
		os.Exit(2)
	}
	log, _ := zap.NewProduction()
	srv, err := mockserver.New(mockserver.Config{
		Dir:             *dir,
		InlineThreshold: *inline,
		MaxUploadBytes:  *maxUpload,
		S3Endpoint:      *s3Endpoint,
		S3Bucket:        *s3Bucket,
		S3AK:            *s3AK,
		S3SK:            *s3SK,
		Logger:          log,
	})
	if err != nil {
		log.Fatal("mock server init failed", zap.Error(err))
	}
	defer srv.Close()
	log.Info("drive9-mock-server listening", zap.String("addr", *addr), zap.String("dir", *dir))
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		log.Fatal("listen failed", zap.Error(err))
	}
}
