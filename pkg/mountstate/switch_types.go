package mountstate

import "github.com/mem9-ai/drive9/pkg/viewprotocol"

const (
	SwitchRecovering = "RECOVERING"
	SwitchDone       = "DONE"
)

// SwitchReceipt describes a durable binding decision. RECOVERING means ordinary
// I/O must stay closed until the caller has installed the target and invalidated
// its caches. DONE does not make it safe to resume a crashed FUSE process.
type SwitchReceipt struct {
	OperationID    string               `json:"operation_id"`
	FromGeneration uint64               `json:"from_generation"`
	Generation     uint64               `json:"generation"`
	From           viewprotocol.Binding `json:"from"`
	Target         viewprotocol.Binding `json:"target"`
	State          string               `json:"state"`
}

// SwitchSnapshot reports the selected target even when installation is pending.
// It never falls back to main after a durable switch decision.
type SwitchSnapshot struct {
	MountID    string               `json:"mount_id"`
	Generation uint64               `json:"generation"`
	Binding    viewprotocol.Binding `json:"binding"`
	Pending    *SwitchReceipt       `json:"pending,omitempty"`
}
