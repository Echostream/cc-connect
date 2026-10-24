//go:build !(linux || darwin)

package core

import "os"

type fileIdentity struct {
	dev     uint64
	ino     uint64
	uid     uint32
	size    int64
	mode    os.FileMode
	regular bool
	dir     bool
}

func identityFromInfo(os.FileInfo) (fileIdentity, error) {
	return fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
}

func identityFromRoot(*os.Root, string) (fileIdentity, error) {
	return fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
}

func (id fileIdentity) sameFile(other fileIdentity) bool { return false }

func currentEUID() uint32 { return 0 }

func openRootCreateNoFollow(*os.Root, string) (*os.File, error) {
	return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
}

func openRootRegularNoFollow(*os.Root, string) (*os.File, error) {
	return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
}

func fsyncRootDir(*os.Root, string) error {
	return stagedAttachmentError{reason: stagedReasonWriteFailed}
}
