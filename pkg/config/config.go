package config

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Layer 是可參與設定合併的資料來源。Load 的回傳值會被 canonicalize 並與
// 其他 layer 依傳入順序合併；實作必須遵守 context cancellation contract。
type Layer interface {
	Name() string
	Load(context.Context) (map[string]any, error)
}

// Snapshot 是建立後唯讀的設定 view，可安全地被多個 goroutine bind。
type Snapshot interface {
	Bind(string, any, ...BindOption) error
	Has(string) bool
}

// ConfigInputs is the resolved, product-facing set of local configuration
// documents. MergedPaths are applied in order; SourcePaths remain independent
// named documents.
type ConfigInputs struct {
	MergedPaths []string
	SourcePaths []NamedConfigPath
}

// NamedConfigPath 將穩定的 logical source name 對應到本機設定檔路徑。
type NamedConfigPath struct {
	Name string
	Path string
}

// SourceSnapshot 除了 merged tree，也能依 logical source name 綁定獨立文件。
type SourceSnapshot interface {
	Snapshot
	// HasSource reports whether a named source was successfully loaded into
	// this snapshot. It does not read the filesystem or validate its contents.
	HasSource(name string) bool
	BindSource(name string, path string, target any, opts ...BindOption) error
}

// Format 是 file layer 使用的文件格式。
type Format string

const (
	// FormatAuto 依檔案副檔名選擇格式。
	FormatAuto Format = ""
	// FormatJSON 表示 JSON 文件。
	FormatJSON Format = "json"
	// FormatYAML 表示 YAML 文件。
	FormatYAML Format = "yaml"
	// FormatXML 表示 XML 文件。
	FormatXML Format = "xml"
)

// FileOption 調整 file layer 的讀取行為。
type FileOption interface{ applyFile(*fileOptions) }

// BindOption 調整 Snapshot.Bind 的投影行為。
type BindOption interface{ applyBind(*bindOptions) }
type fileOptions struct {
	optional bool
	format   Format
}
type bindOptions struct{ strict bool }
type optionalOption struct{}

func (optionalOption) applyFile(o *fileOptions) { o.optional = true }

type formatOption Format

func (f formatOption) applyFile(o *fileOptions) { o.format = Format(f) }

type strictOption struct{}

func (strictOption) applyBind(o *bindOptions) { o.strict = true }

// Optional 讓不存在的 file layer 回傳空設定；其他讀檔或 parse error 仍會回傳。
func Optional() FileOption { return optionalOption{} }

// WithFormat 明確指定 file layer 的格式，覆蓋副檔名自動判斷。
func WithFormat(f Format) FileOption { return formatOption(f) }

// Strict 要求 Bind target struct 拒絕來源中的未知設定欄位。
func Strict() BindOption { return strictOption{} }

// Load 依傳入順序載入、canonicalize 並合併 layers。
func Load(ctx context.Context, layers ...Layer) (Snapshot, error) {
	if ctx == nil {
		return nil, fmt.Errorf("load config: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	var envCount int
	root := map[string]any{}
	for _, l := range layers {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("load config layer: %w", err)
		}
		if isNilInterface(l) {
			return nil, fmt.Errorf("load config: nil layer")
		}
		layerName := l.Name()
		if strings.TrimSpace(layerName) == "" {
			return nil, fmt.Errorf("load config: layer name is empty")
		}
		if _, ok := l.(*envLayer); ok {
			envCount++
			if envCount > 1 {
				return nil, fmt.Errorf("load config: multiple environment layers are not allowed")
			}
		}
		v, err := l.Load(ctx)
		if err != nil {
			return nil, fmt.Errorf("load config layer %q: %w", layerName, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("load config layer %q: %w", layerName, err)
		}
		cv, err := canonicalMap(v)
		if err != nil {
			return nil, fmt.Errorf("load config layer %q: %w", layerName, err)
		}
		if err := mergeInto(root, cv, ""); err != nil {
			return nil, fmt.Errorf("merge config layer %q: %w", layerName, err)
		}
	}
	// canonicalization 與 merge 不是可取消操作；完成所有 layer 後再檢查，
	// 避免 context 在 CPU 工作期間取消後仍公開成功 snapshot。
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &snapshot{tree: root}, nil
}

// isNilInterface 攔截 nil interface 與包在 interface 內的 typed-nil value，
// 避免 Layer/Snapshot 等公開 interface 在後續 method call 時 panic。
func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// splitPath 驗證公開 dot path 並拆成 lookup segments。
func splitPath(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	p := strings.Split(path, ".")
	for _, s := range p {
		if s == "" {
			return nil, fmt.Errorf("invalid config path %q", path)
		}
	}
	return p, nil
}

// lookupPath 依公開 dot path 從 canonical tree 取得 value。
func lookupPath(root map[string]any, path string) (any, bool, error) {
	parts, e := splitPath(path)
	if e != nil {
		return nil, false, e
	}
	if len(parts) == 0 {
		return root, true, nil
	}
	var cur any = root
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false, nil
		}
		cur, ok = m[p]
		if !ok {
			return nil, false, nil
		}
	}
	return cur, true, nil
}
