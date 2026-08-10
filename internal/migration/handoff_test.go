package migration

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const (
	sampleConfigPath     = "../../docs/examples/drive9-migration/config.yaml"
	kubernetesSamplePath = "../../docs/examples/drive9-migration/kubernetes.yaml"
	runbookPath          = "../../docs/guides/drive9-migration-v1-runbook.md"
)

func TestOperatorSampleSupportsBothLayoutsAndContainsNoCredential(t *testing.T) {
	cfg, err := LoadConfig(sampleConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateMappings(cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Jobs) != 3 || cfg.Jobs[0].Target.Prefix != "/" || cfg.Jobs[1].Target.SpaceRef != cfg.Jobs[2].Target.SpaceRef || cfg.Jobs[1].Target.Prefix == cfg.Jobs[2].Target.Prefix {
		t.Fatalf("sample mappings=%+v", cfg.Jobs)
	}
	body, err := os.ReadFile(sampleConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"api_key", "Bearer ", "secret:"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("sample contains forbidden credential material %q", forbidden)
		}
	}
}

func TestPlanHandoffSchemaIsNonSensitiveAndRunbookCoversAcceptedWorkflow(t *testing.T) {
	result := PreflightResult{VolumeID: "vol-0b", NodeName: "migration-node-b", SourceRoot: "/mnt/ebs/vol-0b", SpaceRef: "shared", Prefix: "/vol-0b", CredentialRef: "shared-owner-key", RequiredCapabilities: true}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"volume_id", "node_name", "source_root", "space_ref", "prefix", "credential_ref", "required_capabilities"} {
		if !strings.Contains(string(body), `"`+field+`"`) {
			t.Fatalf("plan handoff omitted %q: %s", field, body)
		}
	}
	if strings.Contains(string(body), "api_key") || strings.Contains(string(body), "Bearer ") {
		t.Fatalf("plan handoff leaked credential material: %s", body)
	}
	runbook, err := os.ReadFile(runbookPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(runbook)
	for _, required := range []string{"T0", "T1", "T2", "ConfigMap", "rollout-restart", "verify-full", "prepare-drive9-cutover", "post-T0", "residue", "metadata", "per Job", "residual ABA", "one mounted, read-only EBS Source per Job", "no rollback", "exact per-Job control directory", "kubectl-drive9-migration", "expected_job_ids_json", ".worker_status.fence_complete == true", "fsGroup", "recursively change Source ownership"} {
		if !strings.Contains(text, required) {
			t.Fatalf("runbook omitted %q", required)
		}
	}
}

func TestKubernetesSampleMatchesPluginAndRestrictedRootContract(t *testing.T) {
	body, err := os.ReadFile(kubernetesSamplePath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"app.kubernetes.io/component: worker",
		"app.kubernetes.io/instance: single-pvc-trial",
		"- name: drive9-migration",
		"allowPrivilegeEscalation: false",
		"readOnlyRootFilesystem: true",
		"capabilities:\n              drop:\n                - ALL",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("Kubernetes sample omitted %q", required)
		}
	}
	if strings.Count(text, "runAsUser: 0") != 2 || strings.Count(text, "runAsGroup: 0") != 2 {
		t.Fatalf("Kubernetes sample must apply the documented root exception to plan and Worker")
	}
}
