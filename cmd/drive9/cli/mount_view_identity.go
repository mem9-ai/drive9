package cli

import (
	"errors"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type mountViewIDFlag string

func (id *mountViewIDFlag) String() string { return string(*id) }

func (id *mountViewIDFlag) Set(value string) error {
	if *id != "" {
		return errors.New("mount-view-id must occur exactly once")
	}
	if err := viewprotocol.ValidateOperationID(value); err != nil {
		return err
	}
	*id = mountViewIDFlag(value)
	return nil
}
