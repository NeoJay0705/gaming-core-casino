package config

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type fileLayer struct {
	path    string
	options fileOptions
}

// loadedFileDocument 是一次讀檔的結果。將 parsed tree 與原始 bytes 綁在一起，
// 才能從實際放入 snapshot 的同一份內容計算 digest。
type loadedFileDocument struct {
	tree  map[string]any
	bytes []byte
}

// File 建立從 path 讀取 JSON、YAML 或 XML 的 file layer。
func File(path string, opts ...FileOption) Layer {
	o := fileOptions{}
	for _, x := range opts {
		if x != nil {
			x.applyFile(&o)
		}
	}
	return &fileLayer{path: path, options: o}
}
func (f *fileLayer) Name() string { return "file:" + f.path }
func (f *fileLayer) Load(ctx context.Context) (map[string]any, error) {
	document, err := f.loadDocument(ctx)
	if err != nil {
		return nil, err
	}
	return document.tree, nil
}

func (f *fileLayer) loadDocument(ctx context.Context) (loadedFileDocument, error) {
	if f.path == "" {
		return loadedFileDocument{}, fmt.Errorf("file path is empty")
	}
	if ctx == nil {
		return loadedFileDocument{}, fmt.Errorf("nil context")
	}
	if err := ctx.Err(); err != nil {
		return loadedFileDocument{}, err
	}
	fmtx := f.options.format
	if fmtx == FormatAuto {
		switch strings.ToLower(filepath.Ext(f.path)) {
		case ".json":
			fmtx = FormatJSON
		case ".yaml", ".yml":
			fmtx = FormatYAML
		case ".xml":
			fmtx = FormatXML
		default:
			return loadedFileDocument{}, fmt.Errorf("unsupported format for %q", f.path)
		}
	}
	if fmtx != FormatJSON && fmtx != FormatYAML && fmtx != FormatXML {
		return loadedFileDocument{}, fmt.Errorf("invalid format %q", fmtx)
	}
	b, err := os.ReadFile(f.path)
	// ReadFile 不支援 context cancellation；完成 system call 後先檢查，
	// 避免 optional file 在 context 已取消時仍被視為成功載入。
	if contextErr := ctx.Err(); contextErr != nil {
		return loadedFileDocument{}, contextErr
	}
	if err != nil {
		if f.options.optional && errors.Is(err, fs.ErrNotExist) {
			return loadedFileDocument{tree: map[string]any{}}, nil
		}
		return loadedFileDocument{}, fmt.Errorf("read %s: %w", f.path, err)
	}
	var v map[string]any
	switch fmtx {
	case FormatJSON:
		v, err = parseJSON(b)
	case FormatYAML:
		v, err = parseYAML(b)
	case FormatXML:
		v, err = parseXML(b)
	}
	if err != nil {
		return loadedFileDocument{}, fmt.Errorf("parse %s: %w", fmtx, err)
	}
	// parser 同樣不可取消；回傳前再次確認 context，避免 caller 收到已取消
	// request 的成功 document。
	if contextErr := ctx.Err(); contextErr != nil {
		return loadedFileDocument{}, contextErr
	}
	return loadedFileDocument{tree: v, bytes: b}, nil
}

type envLayer struct{ prefix string }

// Env 建立以雙底線分隔 nested key 的 environment layer。
func Env(prefix string) Layer    { return &envLayer{prefix: prefix} }
func (e *envLayer) Name() string { return "env:" + e.prefix }
func (e *envLayer) Load(ctx context.Context) (map[string]any, error) {
	if e.prefix == "" {
		return nil, fmt.Errorf("environment prefix is empty")
	}
	if ctx == nil {
		return nil, fmt.Errorf("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type pair struct{ k, v string }
	var a []pair
	for _, x := range os.Environ() {
		p := strings.IndexByte(x, '=')
		if p >= 0 && strings.HasPrefix(x[:p], e.prefix) {
			a = append(a, pair{x[:p], x[p+1:]})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(a, func(i, j int) bool { return a[i].k < a[j].k })
	r := map[string]any{}
	for _, x := range a {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rest := strings.TrimPrefix(x.k, e.prefix)
		if rest == "" || strings.HasPrefix(rest, "__") || strings.HasSuffix(rest, "__") || strings.Contains(rest, "____") {
			return nil, fmt.Errorf("invalid environment key")
		}
		parts := strings.Split(rest, "__")
		for i := range parts {
			parts[i] = strings.ToLower(parts[i])
			if parts[i] == "" {
				return nil, fmt.Errorf("invalid environment key")
			}
		}
		var cur = r
		for _, p := range parts[:len(parts)-1] {
			v, ok := cur[p]
			if ok {
				m, ok := v.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("environment path conflict")
				}
				cur = m
			} else {
				m := map[string]any{}
				cur[p] = m
				cur = m
			}
		}
		leaf := parts[len(parts)-1]
		if _, ok := cur[leaf]; ok {
			return nil, fmt.Errorf("environment path conflict")
		}
		cur[leaf] = x.v
	}
	return r, nil
}

func parseJSON(b []byte) (map[string]any, error) {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	v, err := decodeJSONValue(d)
	if err != nil {
		return nil, err
	}
	var extra any
	if err := d.Decode(&extra); err == nil {
		return nil, fmt.Errorf("multiple documents")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("root must be object")
	}
	return canonicalMap(m)
}

func decodeJSONValue(d *json.Decoder) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch x := t.(type) {
	case json.Delim:
		switch x {
		case '{':
			m := map[string]any{}
			for d.More() {
				kt, e := d.Token()
				if e != nil {
					return nil, e
				}
				k, ok := kt.(string)
				if !ok {
					return nil, fmt.Errorf("object key is not string")
				}
				if _, ok := m[k]; ok {
					return nil, fmt.Errorf("duplicate key")
				}
				v, e := decodeJSONValue(d)
				if e != nil {
					return nil, e
				}
				m[k] = v
			}
			if _, err := d.Token(); err != nil {
				return nil, err
			}
			return m, nil
		case '[':
			a := []any{}
			for d.More() {
				v, e := decodeJSONValue(d)
				if e != nil {
					return nil, e
				}
				a = append(a, v)
			}
			if _, err := d.Token(); err != nil {
				return nil, err
			}
			return a, nil
		}
	}
	return t, nil
}
func parseYAML(b []byte) (map[string]any, error) {
	d := yaml.NewDecoder(strings.NewReader(string(b)))
	var n yaml.Node
	if err := d.Decode(&n); err != nil {
		return nil, err
	}
	if len(n.Content) != 1 {
		return nil, fmt.Errorf("multiple documents")
	}
	var extra yaml.Node
	if err := d.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple documents")
		}
		return nil, err
	}
	v, err := yamlNode(n.Content[0], map[*yaml.Node]bool{})
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("root must be object")
	}
	return canonicalMap(m)
}

// yamlNode 將 YAML node 轉成 canonical value。anchor 與 alias 會直接展開；
// seen 追蹤目前展開中的 anchor，讓 recursive anchor（例如
// `a: &x [*x]`）回傳錯誤而不是無限遞迴。每次展開都建立新的 map/slice，
// 因此結果不會保留 YAML alias reference。
func yamlNode(n *yaml.Node, seen map[*yaml.Node]bool) (any, error) {
	switch n.Kind {
	case yaml.AliasNode:
		if n.Alias == nil {
			return nil, fmt.Errorf("invalid YAML alias")
		}
		if seen[n.Alias] {
			return nil, fmt.Errorf("recursive YAML anchor")
		}
		seen[n.Alias] = true
		defer delete(seen, n.Alias)
		return yamlNode(n.Alias, seen)
	case yaml.MappingNode:
		m := map[string]any{}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			if k.Tag != "!!str" {
				return nil, fmt.Errorf("map key is not string")
			}
			if _, ok := m[k.Value]; ok {
				return nil, fmt.Errorf("duplicate key")
			}
			v, e := yamlNode(n.Content[i+1], seen)
			if e != nil {
				return nil, e
			}
			m[k.Value] = v
		}
		return m, nil
	case yaml.SequenceNode:
		a := make([]any, len(n.Content))
		for i, x := range n.Content {
			v, e := yamlNode(x, seen)
			if e != nil {
				return nil, e
			}
			a[i] = v
		}
		return a, nil
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return nil, nil
		}
		if n.Tag == "!!bool" {
			var v bool
			e := n.Decode(&v)
			return v, e
		}
		if strings.HasPrefix(n.Tag, "!!int") {
			var v int64
			if e := n.Decode(&v); e == nil {
				return v, nil
			}
			var u uint64
			if e := n.Decode(&u); e == nil {
				return u, nil
			}
			return nil, fmt.Errorf("invalid integer")
		}
		if n.Tag == "!!float" {
			var v float64
			if e := n.Decode(&v); e != nil || v != v {
				return nil, fmt.Errorf("invalid float")
			}
			return v, nil
		}
		return n.Value, nil
	}
	return nil, fmt.Errorf("unsupported YAML node")
}
func parseXML(b []byte) (map[string]any, error) {
	d := xml.NewDecoder(strings.NewReader(string(b)))
	var root *xmlNode
	var stack []*xmlNode
	for {
		t, e := d.Token()
		if e != nil {
			if errors.Is(e, io.EOF) {
				break
			}
			return nil, e
		}
		switch x := t.(type) {
		case xml.StartElement:
			n := &xmlNode{name: x.Name.Local, attrs: x.Attr}
			if len(stack) == 0 {
				if root != nil {
					return nil, fmt.Errorf("multiple roots")
				}
				root = n
			} else {
				stack[len(stack)-1].children = append(stack[len(stack)-1].children, n)
			}
			stack = append(stack, n)
		case xml.EndElement:
			if len(stack) == 0 {
				return nil, fmt.Errorf("unexpected end")
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				stack[len(stack)-1].text += string(x)
			}
		case xml.Comment, xml.ProcInst:
		default:
		}
	}
	if root == nil || len(stack) != 0 {
		return nil, fmt.Errorf("invalid XML root")
	}
	v, e := root.value()
	if e != nil {
		return nil, e
	}
	return map[string]any{root.name: v}, nil
}

type xmlNode struct {
	name     string
	attrs    []xml.Attr
	children []*xmlNode
	text     string
}

func (n *xmlNode) value() (any, error) {
	m := map[string]any{}
	for _, a := range n.attrs {
		m["@"+a.Name.Local] = a.Value
	}
	text := strings.TrimSpace(n.text)
	if len(n.children) == 0 {
		if len(m) == 0 {
			return text, nil
		}
		if text != "" {
			m["#text"] = text
		}
		return m, nil
	}
	if text != "" {
		return nil, fmt.Errorf("mixed content at %s", n.name)
	}
	for _, c := range n.children {
		v, e := c.value()
		if e != nil {
			return nil, e
		}
		if old, ok := m[c.name]; ok {
			if a, ok := old.([]any); ok {
				m[c.name] = append(a, v)
			} else {
				m[c.name] = []any{old, v}
			}
		} else {
			m[c.name] = v
		}
	}
	return m, nil
}
