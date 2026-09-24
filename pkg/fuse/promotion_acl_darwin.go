//go:build darwin

package fuse

import (
	"fmt"
	"sync"
	"syscall"
	"unsafe"

	"github.com/ebitengine/purego"
)

const aclTypeExtended = 0x100

var (
	promotionACLOnce    sync.Once
	promotionACLGetFile uintptr
	promotionACLFree    uintptr
	promotionACLLoadErr error
)

func loadPromotionACLFunctions() {
	promotionACLGetFile, promotionACLLoadErr = purego.Dlsym(purego.RTLD_DEFAULT, "acl_get_file")
	if promotionACLLoadErr != nil {
		return
	}
	promotionACLFree, promotionACLLoadErr = purego.Dlsym(purego.RTLD_DEFAULT, "acl_free")
}

func validatePromotionPlatformACL(abs string) error {
	promotionACLOnce.Do(loadPromotionACLFunctions)
	if promotionACLLoadErr != nil {
		return fmt.Errorf("load acl functions: %w", promotionACLLoadErr)
	}
	path := append([]byte(abs), 0)
	acl, _, errno := purego.SyscallN(
		promotionACLGetFile,
		uintptr(unsafe.Pointer(&path[0])),
		uintptr(aclTypeExtended),
	)
	if acl != 0 {
		_, _, _ = purego.SyscallN(promotionACLFree, acl)
		return fmt.Errorf("acl: %s", abs)
	}
	aclErr := syscall.Errno(int32(errno))
	if aclErr == syscall.ENOENT {
		return nil
	}
	return fmt.Errorf("read acl %s: %w", abs, aclErr)
}
