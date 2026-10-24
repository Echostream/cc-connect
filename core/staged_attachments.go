package core

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	stagedReasonTooLarge        = "attachment_too_large"
	stagedReasonWriteFailed     = "attachment_write_failed"
	stagedReasonInvalidName     = "attachment_invalid_name"
	stagedReasonUnsupportedType = "attachment_unsupported_type"
)

// StagedAttachment is the public, non-sensitive metadata for a staged inbound
// attachment. Path is an absolute path inside workDir/.cc-connect/attachments;
// callers must not log it to untrusted sinks.
type StagedAttachment struct {
	Path     string
	Kind     string // image|file
	MimeType string
	Size     int64
}

type stagedAttachmentError struct{ reason string }

func (e stagedAttachmentError) Error() string  { return e.reason }
func (e stagedAttachmentError) Reason() string { return e.reason }

// StagedAttachments owns one unique per-message staging directory. The directory
// and its parent are kept open so write/read/cleanup do not need to re-resolve
// attacker-controlled absolute paths after staging starts. The trusted computing
// base is the wxbridge uid/root plus existing staging ancestors that pass owner
// and mode checks. Linux POSIX ACL write grants are expected to affect the
// group-class mask bit and be caught by the mode check; same-uid processes,
// root, or platform ACL models not reflected in mode bits remain deployment TCB.
type StagedAttachments struct {
	dirAbs     string
	dirName    string
	dirRoot    *os.Root
	parentRoot *os.Root
	dirID      fileIdentity
	entries    []stagedAttachmentEntry

	cleanupBeforeRemove func() error
	cleanupOnce         sync.Once
	cleanupErr          error
	mu                  sync.Mutex
}

type stagedAttachmentEntry struct {
	public StagedAttachment
	rel    string
	id     fileIdentity
	hash   [sha256.Size]byte
}

type stagedFileOps struct {
	beforeCreateFile         func(index int, name string) error
	afterValidateBeforeOpen  func(component string) error
	afterMkdirBeforeValidate func(component string) error
	writeFile                func(index int, file *os.File, data []byte) (int, error)
	afterWrite               func(index int, path string, file *os.File) error
	syncFile                 func(index int, file *os.File) error
	closeFile                func(index int, file *os.File) error
	fixedNonce               string
	maxBytesCeiling          int64
}

func defaultStagedFileOps() stagedFileOps { return stagedFileOps{} }

// StageMessageAttachments validates and writes all image/file attachments in a
// single transaction. On success, ownership moves to msg.Staged and the raw
// ImageAttachment/FileAttachment byte slices are released from msg. On any
// failure, msg is left unchanged and the owned staging directory is removed when
// doing so can be proven to target the original owned directory.
func StageMessageAttachments(workDir string, msg *Message, maxBytes int64) (*StagedAttachments, error) {
	return stageMessageAttachmentsWithOps(workDir, msg, maxBytes, defaultStagedFileOps())
}

func stageMessageAttachmentsWithOps(workDir string, msg *Message, maxBytes int64, ops stagedFileOps) (*StagedAttachments, error) {
	if msg == nil || (len(msg.Images) == 0 && len(msg.Files) == 0) {
		return nil, nil
	}
	maxBytes = effectiveStagedMaxBytes(maxBytes, ops.maxBytesCeiling)
	if maxBytes <= 0 {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	absWorkDir, err := filepath.Abs(workDir)
	if err != nil || absWorkDir == "" {
		return nil, stagedAttachmentError{reason: stagedReasonInvalidName}
	}
	if err := validateAttachmentPayloads(msg, maxBytes); err != nil {
		return nil, err
	}

	staged, err := createOwnedStagingDir(absWorkDir, msg.MessageID, ops)
	if err != nil {
		return nil, err
	}
	owned := true
	defer func() {
		if owned {
			_ = staged.Cleanup()
		}
	}()

	index := 0
	for i, image := range msg.Images {
		name := sanitizeAttachmentFileName(image.FileName)
		if name == "" {
			name = fmt.Sprintf("image_%d%s", i, ExtFromMime(image.MimeType))
		}
		entry, err := staged.writeOne(index, name, "image", image.MimeType, image.Data, ops)
		if err != nil {
			return nil, err
		}
		staged.entries = append(staged.entries, entry)
		index++
	}
	for i, file := range msg.Files {
		name := sanitizeAttachmentFileName(file.FileName)
		if name == "" {
			name = fmt.Sprintf("file_%d", i)
		}
		mimeType := file.MimeType
		if strings.TrimSpace(mimeType) == "" {
			mimeType = "application/octet-stream"
		}
		entry, err := staged.writeOne(index, name, "file", mimeType, file.Data, ops)
		if err != nil {
			return nil, err
		}
		staged.entries = append(staged.entries, entry)
		index++
	}
	if err := fsyncRootDir(staged.dirRoot, "."); err != nil {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	if err := staged.ensureDirIdentityLocked(); err != nil {
		return nil, err
	}

	owned = false
	msg.Staged = staged
	msg.Images = nil
	msg.Files = nil
	return staged, nil
}

func effectiveStagedMaxBytes(maxBytes, testCeiling int64) int64 {
	ceiling := DefaultMaxAttachmentSize
	if testCeiling > 0 {
		ceiling = testCeiling
	}
	if maxBytes > ceiling {
		return ceiling
	}
	return maxBytes
}

func validateAttachmentPayloads(msg *Message, maxBytes int64) error {
	for _, image := range msg.Images {
		if len(image.Data) == 0 || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(image.MimeType)), "image/") {
			return stagedAttachmentError{reason: stagedReasonUnsupportedType}
		}
		if int64(len(image.Data)) > maxBytes {
			return stagedAttachmentError{reason: stagedReasonTooLarge}
		}
	}
	for _, file := range msg.Files {
		if len(file.Data) == 0 {
			return stagedAttachmentError{reason: stagedReasonUnsupportedType}
		}
		if int64(len(file.Data)) > maxBytes {
			return stagedAttachmentError{reason: stagedReasonTooLarge}
		}
	}
	return nil
}

func createOwnedStagingDir(absWorkDir, messageID string, ops stagedFileOps) (*StagedAttachments, error) {
	workRoot, err := os.OpenRoot(absWorkDir)
	if err != nil {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	defer workRoot.Close()

	ccRoot, _, err := openVerifiedTrustedRoot(workRoot, ".cc-connect", ".cc-connect", ops)
	if err != nil {
		return nil, err
	}
	defer ccRoot.Close()
	attachmentsRoot, _, err := openVerifiedTrustedRoot(ccRoot, "attachments", "attachments", ops)
	if err != nil {
		return nil, err
	}
	closeAttachments := true
	defer func() {
		if closeAttachments {
			_ = attachmentsRoot.Close()
		}
	}()

	safeID := sanitizeAttachmentFileName(messageID)
	if safeID == "" {
		safeID = "message"
	}
	for attempt := 0; attempt < 16; attempt++ {
		dirName, err := stagingDirName(safeID, ops.fixedNonce)
		if err != nil {
			return nil, err
		}
		dirRoot, dirID, err := createVerifiedOwnedRoot(attachmentsRoot, dirName, "message", ops)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return nil, err
		}
		closeDir := true
		defer func() {
			if closeDir {
				_ = dirRoot.Close()
			}
		}()
		closeAttachments = false
		closeDir = false
		return &StagedAttachments{
			dirAbs:     filepath.Join(absWorkDir, ".cc-connect", "attachments", dirName),
			dirName:    dirName,
			dirRoot:    dirRoot,
			parentRoot: attachmentsRoot,
			dirID:      dirID,
		}, nil
	}
	return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
}

func openVerifiedTrustedRoot(parent *os.Root, component, hookName string, ops stagedFileOps) (*os.Root, fileIdentity, error) {
	return openVerifiedRoot(parent, component, hookName, ops, false)
}

func createVerifiedOwnedRoot(parent *os.Root, component, hookName string, ops stagedFileOps) (*os.Root, fileIdentity, error) {
	return openVerifiedRoot(parent, component, hookName, ops, true)
}

func openVerifiedRoot(parent *os.Root, component, hookName string, ops stagedFileOps, requireCreated bool) (*os.Root, fileIdentity, error) {
	if component == "" || component == "." || component == ".." || strings.Contains(component, "/") || strings.Contains(component, `\\`) {
		return nil, fileIdentity{}, stagedAttachmentError{reason: stagedReasonInvalidName}
	}
	created := false
	if err := parent.Mkdir(component, 0o755); err != nil {
		if errors.Is(err, os.ErrExist) && requireCreated {
			return nil, fileIdentity{}, os.ErrExist
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
	} else {
		created = true
	}
	if ops.afterMkdirBeforeValidate != nil {
		if err := ops.afterMkdirBeforeValidate(hookName); err != nil {
			return nil, fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
	}
	preID, err := verifyTrustedRootComponent(parent, component)
	if err != nil {
		return nil, fileIdentity{}, err
	}
	if ops.afterValidateBeforeOpen != nil {
		if err := ops.afterValidateBeforeOpen(hookName); err != nil {
			return nil, fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
	}
	opened, err := parent.OpenRoot(component)
	if err != nil {
		return nil, fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	openedID, err := verifyTrustedRootComponent(opened, ".")
	if err != nil || !openedID.sameFile(preID) || openedID.uid != preID.uid || openedID.mode != preID.mode {
		_ = opened.Close()
		if requireCreated && created {
			_ = removeRootComponentIfIdentityMatches(parent, component, preID)
		}
		return nil, fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	if err := fsyncRootDir(parent, "."); err != nil {
		_ = opened.Close()
		if requireCreated && created {
			_ = removeRootComponentIfIdentityMatches(parent, component, preID)
		}
		return nil, fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	return opened, openedID, nil
}

func removeRootComponentIfIdentityMatches(parent *os.Root, component string, want fileIdentity) error {
	current, err := identityFromRoot(parent, component)
	if err != nil || !current.sameFile(want) || current.uid != want.uid || current.mode != want.mode {
		return stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	return parent.RemoveAll(component)
}

func verifyTrustedRootComponent(root *os.Root, rel string) (fileIdentity, error) {
	info, err := root.Lstat(rel)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	id, err := identityFromInfo(info)
	if err != nil || !id.dir || id.uid != currentEUID() || info.Mode().Perm()&0o022 != 0 {
		return fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	return id, nil
}

func stagingDirName(safeID, fixedNonce string) (string, error) {
	if fixedNonce != "" {
		return safeID + "-" + fixedNonce, nil
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	return safeID + "-" + hex.EncodeToString(raw), nil
}

func (s *StagedAttachments) writeOne(index int, name, kind, mimeType string, data []byte, ops stagedFileOps) (stagedAttachmentEntry, error) {
	for suffix := 0; ; suffix++ {
		candidate := name
		if suffix > 0 {
			ext := filepath.Ext(name)
			stem := strings.TrimSuffix(name, ext)
			candidate = fmt.Sprintf("%s_%d%s", stem, suffix, ext)
		}
		if ops.beforeCreateFile != nil {
			if err := ops.beforeCreateFile(index, candidate); err != nil {
				return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
			}
		}
		file, err := openRootCreateNoFollow(s.dirRoot, candidate)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
		ok := false
		defer func() {
			if !ok {
				_ = s.dirRoot.Remove(candidate)
			}
		}()
		if err := writeFull(index, file, data, ops); err != nil {
			_ = file.Close()
			return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
		if ops.afterWrite != nil {
			if err := ops.afterWrite(index, filepath.Join(s.dirAbs, candidate), file); err != nil {
				_ = file.Close()
				return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
			}
		}
		if ops.syncFile != nil {
			err = ops.syncFile(index, file)
		} else {
			err = file.Sync()
		}
		if err != nil {
			_ = file.Close()
			return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
		if ops.closeFile != nil {
			err = ops.closeFile(index, file)
		} else {
			err = file.Close()
		}
		if err != nil {
			_ = file.Close()
			return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
		id, err := identityFromRoot(s.dirRoot, candidate)
		if err != nil {
			return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
		if id.size != int64(len(data)) || !id.regular {
			return stagedAttachmentEntry{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
		}
		ok = true
		return stagedAttachmentEntry{
			public: StagedAttachment{Path: filepath.Join(s.dirAbs, candidate), Kind: kind, MimeType: mimeType, Size: int64(len(data))},
			rel:    candidate,
			id:     id,
			hash:   sha256.Sum256(data),
		}, nil
	}
}

func writeFull(index int, file *os.File, data []byte, ops stagedFileOps) error {
	written := 0
	for written < len(data) {
		var n int
		var err error
		if ops.writeFile != nil {
			n, err = ops.writeFile(index, file, data[written:])
		} else {
			n, err = file.Write(data[written:])
		}
		if n > 0 {
			written += n
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Entries returns a defensive copy of public staged metadata.
func (s *StagedAttachments) Entries() []StagedAttachment {
	if s == nil || len(s.entries) == 0 {
		return nil
	}
	out := make([]StagedAttachment, len(s.entries))
	for i, entry := range s.entries {
		out[i] = entry.public
	}
	return out
}

// ReadAll securely re-opens and verifies staged attachments immediately before
// an active Agent turn needs bytes. It does not cache bytes on StagedAttachments.
func (s *StagedAttachments) ReadAll() ([]ImageAttachment, []FileAttachment, error) {
	if s == nil || len(s.entries) == 0 {
		return nil, nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureDirIdentityLocked(); err != nil {
		return nil, nil, err
	}
	images := make([]ImageAttachment, 0)
	files := make([]FileAttachment, 0)
	for _, entry := range s.entries {
		data, err := s.readVerifiedEntry(entry)
		if err != nil {
			return nil, nil, err
		}
		base := filepath.Base(entry.public.Path)
		if entry.public.Kind == "image" {
			images = append(images, ImageAttachment{MimeType: entry.public.MimeType, Data: data, FileName: base})
		} else {
			files = append(files, FileAttachment{MimeType: entry.public.MimeType, Data: data, FileName: base})
		}
	}
	return images, files, nil
}

func (s *StagedAttachments) readVerifiedEntry(entry stagedAttachmentEntry) ([]byte, error) {
	info, err := s.dirRoot.Lstat(entry.rel)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	file, err := openRootRegularNoFollow(s.dirRoot, entry.rel)
	if err != nil {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	openedID, err := identityFromInfo(openedInfo)
	if err != nil || !openedID.sameFile(entry.id) || openedID.size != entry.public.Size || openedID.mode != entry.id.mode {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	data, err := io.ReadAll(io.LimitReader(file, entry.public.Size+1))
	if err != nil || int64(len(data)) != entry.public.Size {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	if sha256.Sum256(data) != entry.hash {
		return nil, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	return data, nil
}

// Cleanup removes the owned staging directory exactly once when the held parent
// still resolves the original directory identity. If the path has been replaced
// or detached, Cleanup fails closed rather than deleting the replacement.
func (s *StagedAttachments) Cleanup() error {
	if s == nil {
		return nil
	}
	s.cleanupOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.ensureDirIdentityLocked(); err != nil {
			s.cleanupErr = err
			return
		}
		if s.cleanupBeforeRemove != nil {
			if err := s.cleanupBeforeRemove(); err != nil {
				s.cleanupErr = stagedAttachmentError{reason: stagedReasonWriteFailed}
				return
			}
		}
		if err := s.ensureDirIdentityLocked(); err != nil {
			s.cleanupErr = err
			return
		}
		if err := s.parentRoot.RemoveAll(s.dirName); err != nil {
			s.cleanupErr = stagedAttachmentError{reason: stagedReasonWriteFailed}
			return
		}
		_ = s.dirRoot.Close()
		_ = s.parentRoot.Close()
	})
	return s.cleanupErr
}

func (s *StagedAttachments) ensureDirIdentityLocked() error {
	if s.dirRoot == nil || s.parentRoot == nil {
		return stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	current, err := identityFromRoot(s.parentRoot, s.dirName)
	if err != nil || !current.sameFile(s.dirID) || !current.dir {
		return stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	return nil
}
