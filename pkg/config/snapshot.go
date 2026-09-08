package config

import (
	"fmt"
	"reflect"
	"strings"
)

type snapshot struct {
	tree    map[string]any
	sources map[string]map[string]any
}

func (s *snapshot) Has(path string) bool {
	if path == "" {
		return len(s.tree) > 0
	}
	_, ok, e := lookupPath(s.tree, path)
	return e == nil && ok
}
func (s *snapshot) Bind(path string, target any, opts ...BindOption) error {
	return s.bindTree(s.tree, path, target, opts...)
}

func (s *snapshot) HasSource(name string) bool {
	if s == nil {
		return false
	}
	_, ok := s.sources[name]
	return ok
}

func (s *snapshot) BindSource(name string, path string, target any, opts ...BindOption) error {
	tree, ok := s.sources[name]
	if !ok {
		return fmt.Errorf("bind source %q path %q: source does not exist", name, path)
	}
	if err := s.bindTree(tree, path, target, opts...); err != nil {
		return fmt.Errorf("bind source %q path %q: %w", name, path, err)
	}
	return nil
}

func (s *snapshot) bindTree(tree map[string]any, path string, target any, opts ...BindOption) error {
	o := bindOptions{}
	for _, x := range opts {
		if x != nil {
			x.applyBind(&o)
		}
	}
	if target == nil {
		return fmt.Errorf("bind config path %q: target is nil", path)
	}
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("bind config path %q: target must be non-nil pointer", path)
	}
	src, ok, e := lookupPath(tree, path)
	if e != nil {
		return e
	}
	if !ok {
		return fmt.Errorf("bind config path %q: path does not exist", path)
	}
	tmp := reflect.New(rv.Elem().Type()).Elem()
	cloneReflectValue(tmp, rv.Elem())
	if e := assignValue(tmp, src, path); e != nil {
		return e
	}
	if o.strict {
		if e := validateStrictUnknown(src, tmp, path); e != nil {
			return e
		}
	}
	rv.Elem().Set(tmp)
	return nil
}

// cloneReflectValue 深拷貝 bind target 的 mutable default，並保留 nil 狀態。
func cloneReflectValue(dst, src reflect.Value) {
	if !src.IsValid() {
		return
	}
	switch src.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			dst.Set(reflect.Zero(src.Type()))
			return
		}
		dst.Set(reflect.New(src.Type().Elem()))
		cloneReflectValue(dst.Elem(), src.Elem())
		return
	case reflect.Interface:
		if src.IsNil() {
			dst.Set(reflect.Zero(src.Type()))
			return
		}
		x := reflect.New(src.Elem().Type()).Elem()
		cloneReflectValue(x, src.Elem())
		dst.Set(x)
		return
	case reflect.Struct:
		dst.Set(src)
		for i := 0; i < src.NumField(); i++ {
			if dst.Field(i).CanSet() {
				cloneReflectValue(dst.Field(i), src.Field(i))
			}
		}
		return
	case reflect.Map:
		if src.IsNil() {
			dst.Set(reflect.Zero(src.Type()))
			return
		}
		dst.Set(reflect.MakeMapWithSize(src.Type(), src.Len()))
		for _, k := range src.MapKeys() {
			v := reflect.New(src.Type().Elem()).Elem()
			cloneReflectValue(v, src.MapIndex(k))
			dst.SetMapIndex(k, v)
		}
		return
	case reflect.Slice:
		if src.IsNil() {
			dst.Set(reflect.Zero(src.Type()))
			return
		}
		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Len()))
		for i := 0; i < src.Len(); i++ {
			cloneReflectValue(dst.Index(i), src.Index(i))
		}
		return
	case reflect.Array:
		dst.Set(src)
		for i := 0; i < src.Len(); i++ {
			cloneReflectValue(dst.Index(i), src.Index(i))
		}
		return
	}
	// 設定 target 的剩餘型別是 scalar；此處不會處理 config tree 的 map/slice
	// reference，因此 shallow set 不會把 snapshot 的 mutable branch 交給 caller。
	dst.Set(src)
}

// assignValue 將 canonical source value 寫入 temporary target value。
func assignValue(dst reflect.Value, src any, path string) error {
	if src == nil {
		switch dst.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		return fmt.Errorf("bind config path %q: cannot assign null", path)
	}
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return assignValue(dst.Elem(), src, path)
	}
	if dst.Kind() == reflect.Interface {
		if dst.Type().NumMethod() != 0 {
			return fmt.Errorf("bind config path %q: cannot convert %s to %s", path, reflect.TypeOf(src), dst.Type())
		}
		dst.Set(reflect.ValueOf(cloneCanonicalValue(src)))
		return nil
	}
	if m, ok := src.(map[string]any); ok {
		return assignMapValue(dst, m, path)
	}
	if a, ok := src.([]any); ok {
		if dst.Kind() != reflect.Slice && dst.Kind() != reflect.Array {
			return fmt.Errorf("bind config path %q: source is slice", path)
		}
		if dst.Kind() == reflect.Array && dst.Len() != len(a) {
			return fmt.Errorf("bind config path %q: array length mismatch", path)
		}
		if dst.Kind() == reflect.Slice {
			dst.Set(reflect.MakeSlice(dst.Type(), len(a), len(a)))
		}
		for i, v := range a {
			if e := assignValue(dst.Index(i), v, indexPath(path, i)); e != nil {
				return e
			}
		}
		return nil
	}
	return assignScalar(dst, src, path)
}

// assignMapValue 將 source object 綁定到 string-keyed map 或 config struct。
func assignMapValue(dst reflect.Value, m map[string]any, path string) error {
	if dst.Kind() == reflect.Map {
		if dst.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("bind config path %q: map key must be string", path)
		}
		dst.Set(reflect.MakeMap(dst.Type()))
		for k, v := range m {
			x := reflect.New(dst.Type().Elem()).Elem()
			if e := assignValue(x, v, joinPath(path, k)); e != nil {
				return e
			}
			dst.SetMapIndex(reflect.ValueOf(k).Convert(dst.Type().Key()), x)
		}
		return nil
	}
	if dst.Kind() != reflect.Struct {
		return fmt.Errorf("bind config path %q: target is not struct", path)
	}
	fields := map[string]reflect.Value{}
	if e := mapConfigFields(dst, fields); e != nil {
		return e
	}
	for k, v := range m {
		f, ok := fields[k]
		if ok {
			if e := assignValue(f, v, joinPath(path, k)); e != nil {
				return e
			}
		}
	}
	return nil
}

// mapConfigFields 建立 struct config tag 到可設定欄位的索引，並拒絕 duplicate tag。
func mapConfigFields(v reflect.Value, out map[string]reflect.Value) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		tag := strings.Split(sf.Tag.Get("config"), ",")[0]
		if tag == "-" {
			continue
		}
		f := v.Field(i)
		if sf.Anonymous && tag == "" && f.Kind() == reflect.Struct {
			if e := mapConfigFields(f, out); e != nil {
				return e
			}
			continue
		}
		if tag == "" {
			tag = snake(sf.Name)
		}
		if _, ok := out[tag]; ok {
			return fmt.Errorf("duplicate config field %q", tag)
		}
		out[tag] = f
	}
	return nil
}

// snake 將 exported field name 轉成能處理 acronym 的 snake_case，例如
// MaxRetries -> max_retries、EndpointURL -> endpoint_url、URLEndpoint ->
// url_endpoint。大寫字母前一個字元不是大寫，或目前是 acronym 的結尾時，
// 才插入分隔線，避免 URL 被拆成 u_r_l。
func snake(s string) string {
	r := []rune(s)
	var b strings.Builder
	for i, c := range r {
		if i > 0 && isUpper(c) {
			prevUpper := isUpper(r[i-1])
			nextLower := i+1 < len(r) && isLower(r[i+1])
			if !prevUpper || nextLower {
				b.WriteByte('_')
			}
		}
		b.WriteRune(toLower(c))
	}
	return b.String()
}
func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }
func isLower(r rune) bool { return r >= 'a' && r <= 'z' }
func toLower(r rune) rune {
	if isUpper(r) {
		return r + ('a' - 'A')
	}
	return r
}
