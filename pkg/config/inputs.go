package config

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

// InputArtifact 描述 named source 實際讀取的檔案。MD5 取自已解析進
// snapshot 的同一份 bytes，避免驗證時再次讀檔造成版本不一致。
type InputArtifact struct {
	// Name 是 named source 的 logical name。
	Name string
	// Path 是實際讀取後整理過的絕對路徑。
	Path string
	// MD5 是讀取 bytes 計算出的 32 碼小寫 hexadecimal digest。
	MD5 string
}

// LoadedInputs 包含合併後的設定 snapshot，以及每個 named source 的檔案資訊。
// map key 是穩定的 logical source name，不是部署環境可能改變的路徑。
type LoadedInputs struct {
	// Snapshot 是合併設定與 named source view。
	Snapshot SourceSnapshot
	// NamedSources 以 logical source name 索引檔案資訊。
	NamedSources map[string]InputArtifact
}

// normalizedConfigInputs 是已完成驗證與路徑正規化的 input contract。
// 將它與 caller 傳入的 ConfigInputs 分開，避免載入流程再次解析原始路徑。
type normalizedConfigInputs struct {
	mergedPaths []string
	sourcePaths []NamedConfigPath
}

// inputDocumentLoader 負責單次 bootstrap 期間的文件 cache 與 context 邊界。
// cache 只存在於本次載入，不跨 request 或 App instance 共用。
type inputDocumentLoader struct {
	ctx       context.Context
	documents map[string]loadedFileDocument
}

func newInputDocumentLoader(ctx context.Context) *inputDocumentLoader {
	return &inputDocumentLoader{
		ctx:       ctx,
		documents: make(map[string]loadedFileDocument),
	}
}

// load 以 normalized absolute path 讀取文件，並保留 parsed tree 與原始 bytes。
// cache hit 仍須檢查 context，避免取消後沿用已載入文件完成流程。
func (l *inputDocumentLoader) load(path string) (loadedFileDocument, error) {
	if err := l.ctx.Err(); err != nil {
		return loadedFileDocument{}, fmt.Errorf("load %q: %w", path, err)
	}
	if document, ok := l.documents[path]; ok {
		return document, nil
	}

	format, err := inputFormat(path)
	if err != nil {
		return loadedFileDocument{}, err
	}
	document, err := (&fileLayer{path: path, options: fileOptions{format: format}}).loadDocument(l.ctx)
	if err != nil {
		return loadedFileDocument{}, fmt.Errorf("load config document %q: %w", path, err)
	}
	// os.ReadFile 與 parse 不是可取消操作；回傳前再檢查一次，避免取消後
	// 把這份 document 放進 cache 或讓 caller 收到成功結果。
	if err := l.ctx.Err(); err != nil {
		return loadedFileDocument{}, fmt.Errorf("load %q: %w", path, err)
	}
	l.documents[path] = document
	return document, nil
}

// LoadInputsWithArtifacts 載入設定並保留 named source 的檔案資訊。
// merged document 與 named source 共用同一份 cache；同一路徑只讀取、解析一次。
// 當 merged configuration 宣告 file_integrity 時，已宣告但不存在的 named source
// 會被視為未部署並略過；呼叫端應再以 VerifyFileIntegrity 驗證實際 source 集合。
func LoadInputsWithArtifacts(ctx context.Context, inputs ConfigInputs, envPrefix string) (LoadedInputs, error) {
	if ctx == nil {
		return LoadedInputs{}, fmt.Errorf("load config inputs: nil context")
	}
	if strings.TrimSpace(envPrefix) == "" {
		return LoadedInputs{}, fmt.Errorf("load config inputs: environment prefix is empty")
	}
	if err := ctx.Err(); err != nil {
		return LoadedInputs{}, fmt.Errorf("load config inputs: %w", err)
	}
	normalized, err := normalizeConfigInputs(inputs)
	if err != nil {
		return LoadedInputs{}, err
	}
	loader := newInputDocumentLoader(ctx)

	merged := map[string]any{}
	for _, path := range normalized.mergedPaths {
		document, err := loader.load(path)
		if err != nil {
			return LoadedInputs{}, err
		}
		if err := mergeInto(merged, document.tree, ""); err != nil {
			return LoadedInputs{}, fmt.Errorf("merge config document %q: %w", path, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return LoadedInputs{}, fmt.Errorf("load config inputs: %w", err)
	}
	env, err := (&envLayer{prefix: envPrefix}).Load(ctx)
	if err != nil {
		return LoadedInputs{}, fmt.Errorf("load config environment: %w", err)
	}
	if err := mergeInto(merged, env, ""); err != nil {
		return LoadedInputs{}, fmt.Errorf("merge config environment: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return LoadedInputs{}, fmt.Errorf("load config inputs: %w", err)
	}

	// file_integrity is also the deployment manifest for named sources. Once
	// it is present, a missing declared path means that source was not included
	// in this deployment; the verifier below will require the manifest to match
	// the resulting actual source set exactly.
	_, integrityDeclared, err := lookupPath(merged, "file_integrity")
	if err != nil {
		return LoadedInputs{}, fmt.Errorf("inspect file integrity configuration: %w", err)
	}

	sources := make(map[string]map[string]any, len(normalized.sourcePaths))
	artifacts := make(map[string]InputArtifact, len(normalized.sourcePaths))
	for _, source := range normalized.sourcePaths {
		document, err := loader.load(source.Path)
		if err != nil {
			if integrityDeclared && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return LoadedInputs{}, fmt.Errorf("load named source %q: %w", source.Name, err)
		}
		// document.tree 只會被 mergeInto 讀取，snapshot 也沒有 mutation API；
		// 不再為沒有隔離效果的 source view 做深層 clone。
		sources[source.Name] = document.tree
		checksum := md5.Sum(document.bytes)
		artifacts[source.Name] = InputArtifact{
			Name: source.Name,
			Path: source.Path,
			MD5:  hex.EncodeToString(checksum[:]),
		}
	}
	if err := ctx.Err(); err != nil {
		return LoadedInputs{}, fmt.Errorf("load config inputs: %w", err)
	}

	return LoadedInputs{
		Snapshot:     &snapshot{tree: merged, sources: sources},
		NamedSources: artifacts,
	}, nil
}

// LoadInputs 載入產品端 JSON/YAML input contract。XML 仍由通用 Load 與
// configbootstrap 支援，但在此 API 會明確拒絕。
func LoadInputs(ctx context.Context, inputs ConfigInputs, envPrefix string) (SourceSnapshot, error) {
	loaded, err := LoadInputsWithArtifacts(ctx, inputs, envPrefix)
	if err != nil {
		return nil, err
	}
	return loaded.Snapshot, nil
}

func normalizeConfigInputs(inputs ConfigInputs) (normalizedConfigInputs, error) {
	if len(inputs.MergedPaths) == 0 {
		return normalizedConfigInputs{}, fmt.Errorf("load config inputs: merged paths are empty")
	}
	mergedPaths := make([]string, len(inputs.MergedPaths))
	seenPaths := make(map[string]struct{}, len(inputs.MergedPaths))
	for index, path := range inputs.MergedPaths {
		if strings.TrimSpace(path) == "" {
			return normalizedConfigInputs{}, fmt.Errorf("load config inputs: merged path is empty")
		}
		abs, err := normalizeInputPath(path)
		if err != nil {
			return normalizedConfigInputs{}, fmt.Errorf("load config inputs: resolve merged path %q: %w", path, err)
		}
		if _, exists := seenPaths[abs]; exists {
			return normalizedConfigInputs{}, fmt.Errorf("load config inputs: duplicate merged path %q", path)
		}
		seenPaths[abs] = struct{}{}
		mergedPaths[index] = abs
	}

	sourcePaths := make([]NamedConfigPath, len(inputs.SourcePaths))
	seenNames := make(map[string]struct{}, len(inputs.SourcePaths))
	for index, source := range inputs.SourcePaths {
		if !isValidNamedSourceName(source.Name) {
			return normalizedConfigInputs{}, fmt.Errorf("load config inputs: invalid named source name %q", source.Name)
		}
		if strings.TrimSpace(source.Path) == "" {
			return normalizedConfigInputs{}, fmt.Errorf("load config inputs: named source %q path is empty", source.Name)
		}
		if _, exists := seenNames[source.Name]; exists {
			return normalizedConfigInputs{}, fmt.Errorf("load config inputs: duplicate named source %q", source.Name)
		}
		seenNames[source.Name] = struct{}{}
		path, err := normalizeInputPath(source.Path)
		if err != nil {
			return normalizedConfigInputs{}, fmt.Errorf("load config inputs: resolve named source %q: %w", source.Name, err)
		}
		sourcePaths[index] = NamedConfigPath{Name: source.Name, Path: path}
	}
	return normalizedConfigInputs{mergedPaths: mergedPaths, sourcePaths: sourcePaths}, nil
}

// normalizeInputPath 將 caller 路徑轉成單次載入可共用的 cache key。
func normalizeInputPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

// isValidNamedSourceName 檢查 logical source name，確保它能穩定作為
// snapshot、artifact 與 file_integrity contract 的 join key。
func isValidNamedSourceName(name string) bool {
	if strings.TrimSpace(name) == "" {
		return false
	}
	for _, r := range name {
		if !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '-') {
			return false
		}
	}
	return true
}

func inputFormat(path string) (Format, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return FormatJSON, nil
	case ".yaml", ".yml":
		return FormatYAML, nil
	case ".xml":
		return FormatAuto, fmt.Errorf("load config inputs: XML is not supported for %q", path)
	default:
		return FormatAuto, fmt.Errorf("load config inputs: unsupported format for %q", path)
	}
}
