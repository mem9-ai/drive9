// Command feishu-notify sends a drive9 e2e product-quality card to Feishu/Lark
// for runs that warrant a push (post-merge/nightly failures and performance
// regressions). It reads the aggregated RunReport JSON produced by e2e-aggregate.
//
// It never fails the build for a configuration gap: if no Feishu transport is
// configured (no FEISHU_WEBHOOK and no FEISHU_APP_* secrets), or the run does not
// warrant notification, it logs the reason and exits 0. A genuine send error
// (network/API) is reported and exits non-zero so the delivery problem is
// visible.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/mem9-ai/drive9/internal/e2ereport"
	"github.com/mem9-ai/drive9/internal/feishu"
)

func main() {
	reportPath := flag.String("report", "", "path to the aggregated RunReport JSON (from e2e-aggregate --out)")
	force := flag.Bool("force", false, "send even if policy would not notify (for testing the wiring)")
	flag.Parse()

	if err := run(*reportPath, *force); err != nil {
		fmt.Fprintln(os.Stderr, "feishu-notify:", err)
		os.Exit(1)
	}
}

func run(reportPath string, force bool) error {
	if reportPath == "" {
		return fmt.Errorf("--report is required")
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		return fmt.Errorf("read report: %w", err)
	}
	var report e2ereport.RunReport
	if err := json.Unmarshal(data, &report); err != nil {
		return fmt.Errorf("parse report: %w", err)
	}

	decision := report.ShouldNotifyFeishu()
	if !decision.Notify && !force {
		fmt.Printf("feishu-notify: no notification needed (%s)\n", decision.Reason)
		return nil
	}

	cfg := feishu.ConfigFromEnv(os.Getenv)
	if cfg.Transport() == feishu.TransportNone {
		fmt.Println("feishu-notify: no Feishu transport configured (set FEISHU_WEBHOOK or FEISHU_APP_ID/SECRET/CHAT_ID); skipping send")
		return nil
	}

	sent, err := feishu.Send(context.Background(), cfg, report.FeishuCard())
	if err != nil {
		return fmt.Errorf("send via %s: %w", cfg.Transport(), err)
	}
	if sent {
		fmt.Printf("feishu-notify: sent via %s (%s)\n", cfg.Transport(), decision.Reason)
	}
	return nil
}
