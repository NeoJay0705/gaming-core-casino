package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// 完整性 contract 即使目前只有 MD5 也保留版本號，未來可在不改變
// source 對應規則的前提下擴充更強的演算法。
const (
	fileIntegrityVersion = 1
	md5HexLength         = 32
)

var (
	// ErrFileIntegrityRequired 表示 caller 要求驗證，但 merged configuration
	// 沒有宣告完整性 contract。
	ErrFileIntegrityRequired = errors.New("file integrity configuration is required")
	// ErrFileIntegrityInvalid 表示完整性 contract 格式錯誤或版本不支援。
	ErrFileIntegrityInvalid = errors.New("invalid file integrity configuration")
	// ErrFileIntegrityMismatch 表示 named source 的實際內容與宣告 digest 不同。
	ErrFileIntegrityMismatch = errors.New("file integrity mismatch")
)

// FileIntegrityConfig 是 merged configuration 中供 named source digest 使用的
// strict contract。
type FileIntegrityConfig struct {
	// Version 目前只接受 1。
	Version int `config:"version"`
	// Sources 是要驗證的 named source 宣告清單。
	Sources []FileIntegritySourceSpec `config:"sources"`
}

// FileIntegritySourceSpec 將穩定的 logical source name 對應到完整檔案 bytes
// 的小寫 MD5 digest。
type FileIntegritySourceSpec struct {
	// Source 必須精確等於 ConfigInputs 中的 named source name。
	Source string `config:"source"`
	// MD5 是預期的小寫 hexadecimal digest。
	MD5 string `config:"md5"`
}

// VerifyFileIntegrity 以 LoadInputsWithArtifacts 產生的檔案資訊驗證
// optional/required 的 file_integrity 區塊。manifest 與 actual named source
// 必須是完全相同的 logical-name 集合，集合通過後才比較 MD5。
//
// source name 是唯一 join key。路徑刻意不放入 contract，因為部署可能搬移或
// 覆寫檔案，但仍應保留相同的 logical source identity。
func VerifyFileIntegrity(snapshot SourceSnapshot, namedSources map[string]InputArtifact, required bool) error {
	if isNilInterface(snapshot) {
		return fmt.Errorf("%w: snapshot is nil", ErrFileIntegrityInvalid)
	}
	if !snapshot.Has("file_integrity") {
		if required {
			return fmt.Errorf("%w: path %q is missing", ErrFileIntegrityRequired, "file_integrity")
		}
		return nil
	}

	var contract FileIntegrityConfig
	if err := snapshot.Bind("file_integrity", &contract, Strict()); err != nil {
		return fmt.Errorf("%w: bind file_integrity: %w", ErrFileIntegrityInvalid, err)
	}
	if contract.Version != fileIntegrityVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrFileIntegrityInvalid, contract.Version)
	}
	if len(contract.Sources) == 0 {
		return fmt.Errorf("%w: sources must not be empty", ErrFileIntegrityInvalid)
	}

	expected := make(map[string]FileIntegritySourceSpec, len(contract.Sources))
	for index, declaration := range contract.Sources {
		name := declaration.Source
		trimmedName := strings.TrimSpace(name)
		if trimmedName == "" || name != trimmedName {
			return fmt.Errorf("%w: sources[%d].source must be a non-empty name", ErrFileIntegrityInvalid, index)
		}
		if _, exists := expected[name]; exists {
			return fmt.Errorf("%w: duplicate source %q", ErrFileIntegrityInvalid, name)
		}
		if !isLowerMD5(declaration.MD5) {
			return fmt.Errorf("%w: source %q has invalid md5", ErrFileIntegrityInvalid, name)
		}
		expected[name] = declaration
	}

	for name, artifact := range namedSources {
		// map key 已經完成來源對應；再檢查欄位可防止外部自行組裝
		// artifacts 時，把不同 logical source 的資訊誤綁進來。
		if artifact.Name != name {
			return fmt.Errorf(
				"%w: source %q artifact name is %q",
				ErrFileIntegrityInvalid,
				name,
				artifact.Name,
			)
		}
	}

	missing, unexpected := sourceSetDiff(expected, namedSources)
	if len(missing) > 0 || len(unexpected) > 0 {
		return fmt.Errorf(
			"%w: source set mismatch: expected_count=%d actual_count=%d missing_sources=%v unexpected_sources=%v",
			ErrFileIntegrityInvalid,
			len(expected),
			len(namedSources),
			missing,
			unexpected,
		)
	}

	// Compare digests only after the expected and actual source sets are an
	// exact match. Sorting makes the first reported mismatch deterministic.
	names := make([]string, 0, len(expected))
	for name := range expected {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		declaration := expected[name]
		artifact := namedSources[name]
		if artifact.MD5 != declaration.MD5 {
			return fmt.Errorf(
				"%w: source %q: expected_md5=%s actual_md5=%s",
				ErrFileIntegrityMismatch,
				name,
				declaration.MD5,
				artifact.MD5,
			)
		}
	}
	return nil
}

func sourceSetDiff(expected map[string]FileIntegritySourceSpec, actual map[string]InputArtifact) (missing, unexpected []string) {
	for name := range expected {
		if _, exists := actual[name]; !exists {
			missing = append(missing, name)
		}
	}
	for name := range actual {
		if _, exists := expected[name]; !exists {
			unexpected = append(unexpected, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return missing, unexpected
}

func isLowerMD5(value string) bool {
	if len(value) != md5HexLength {
		return false
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}
