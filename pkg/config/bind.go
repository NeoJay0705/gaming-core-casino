package config

import (
	"encoding"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"
)

var textType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
var durationType = reflect.TypeOf(time.Duration(0))

// assignScalar 將 canonical scalar 轉成 target 欄位，並保留 conversion path。
func assignScalar(d reflect.Value, s any, path string) error {
	if d.CanAddr() && d.Addr().Type().Implements(textType) {
		if x, ok := s.(string); ok {
			if err := d.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(x)); err != nil {
				return fmt.Errorf("bind config path %q: %w", path, err)
			}
			return nil
		}
	}
	if d.Type() == durationType {
		x, ok := s.(string)
		if !ok {
			return fmt.Errorf("bind config path %q: cannot convert %s to time.Duration", path, reflect.TypeOf(s))
		}
		v, e := time.ParseDuration(x)
		if e != nil {
			return fmt.Errorf("bind config path %q: invalid time.Duration syntax: %w", path, e)
		}
		d.SetInt(int64(v))
		return nil
	}
	x, ok := s.(string)
	if ok {
		switch d.Kind() {
		case reflect.String:
			d.SetString(x)
			return nil
		case reflect.Bool:
			v, e := strconv.ParseBool(x)
			if e == nil {
				d.SetBool(v)
				return nil
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v, e := strconv.ParseInt(x, 10, d.Type().Bits())
			if e == nil {
				d.SetInt(v)
				return nil
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v, e := strconv.ParseUint(x, 10, d.Type().Bits())
			if e == nil {
				d.SetUint(v)
				return nil
			}
		case reflect.Float32, reflect.Float64:
			v, e := strconv.ParseFloat(x, d.Type().Bits())
			if e == nil {
				d.SetFloat(v)
				return nil
			}
		}
		return fmt.Errorf("bind config path %q: cannot convert string to %s", path, d.Type())
	}
	rv := reflect.ValueOf(s)
	switch d.Kind() {
	case reflect.Bool:
		if rv.Kind() != reflect.Bool {
			return fmt.Errorf("bind config path %q: cannot convert %s to %s", path, reflect.TypeOf(s), d.Type())
		}
		d.SetBool(rv.Bool())
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var n int64
		switch rv.Kind() {
		case reflect.Int64:
			n = rv.Int()
		case reflect.Uint64:
			if rv.Uint() > uint64(math.MaxInt64) {
				return fmt.Errorf("bind config path %q: numeric overflow converting %s to %s", path, reflect.TypeOf(s), d.Type())
			}
			n = int64(rv.Uint())
		case reflect.Float64:
			f := rv.Float()
			limit := math.Ldexp(1, d.Type().Bits()-1)
			if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || f < -limit || f >= limit {
				return fmt.Errorf("bind config path %q: lossy conversion from %s to %s", path, reflect.TypeOf(s), d.Type())
			}
			n = int64(f)
		default:
			return fmt.Errorf("bind config path %q: cannot convert %s to %s", path, reflect.TypeOf(s), d.Type())
		}
		if d.OverflowInt(n) {
			return fmt.Errorf("bind config path %q: numeric overflow converting %s to %s", path, reflect.TypeOf(s), d.Type())
		}
		d.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var n uint64
		switch rv.Kind() {
		case reflect.Int64:
			if rv.Int() < 0 {
				return fmt.Errorf("bind config path %q: numeric overflow converting %s to %s", path, reflect.TypeOf(s), d.Type())
			}
			n = uint64(rv.Int())
		case reflect.Uint64:
			n = rv.Uint()
		case reflect.Float64:
			f := rv.Float()
			limit := math.Ldexp(1, d.Type().Bits())
			if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || f < 0 || f >= limit {
				return fmt.Errorf("bind config path %q: lossy conversion from %s to %s", path, reflect.TypeOf(s), d.Type())
			}
			n = uint64(f)
		default:
			return fmt.Errorf("bind config path %q: cannot convert %s to %s", path, reflect.TypeOf(s), d.Type())
		}
		if d.OverflowUint(n) {
			return fmt.Errorf("bind config path %q: numeric overflow converting %s to %s", path, reflect.TypeOf(s), d.Type())
		}
		d.SetUint(n)
		return nil
	case reflect.Float32, reflect.Float64:
		var f float64
		switch rv.Kind() {
		case reflect.Int64:
			f = float64(rv.Int())
		case reflect.Uint64:
			f = float64(rv.Uint())
		case reflect.Float64:
			f = rv.Float()
		default:
			return fmt.Errorf("bind config path %q: cannot convert %s to %s", path, reflect.TypeOf(s), d.Type())
		}
		if math.IsInf(f, 0) || math.IsNaN(f) || d.OverflowFloat(f) {
			return fmt.Errorf("bind config path %q: numeric overflow converting %s to %s", path, reflect.TypeOf(s), d.Type())
		}
		d.SetFloat(f)
		return nil
	}
	return fmt.Errorf("bind config path %q: unsupported conversion", path)
}

// validateStrictUnknown 收集 source 中未對應到 target struct 的設定欄位。
func validateStrictUnknown(src any, dst reflect.Value, path string) error {
	m, ok := src.(map[string]any)
	if !ok {
		return nil
	}
	fields := map[string]reflect.Value{}
	dst = indirectSchemaValue(dst)
	if dst.Kind() != reflect.Struct {
		return nil
	}
	if err := mapConfigFields(dst, fields); err != nil {
		return err
	}
	var bad []string
	for k, v := range m {
		f, ok := fields[k]
		if !ok {
			bad = append(bad, joinPath(path, k))
			continue
		}
		unknown, err := collectUnknownPaths(v, f, joinPath(path, k))
		if err != nil {
			return err
		}
		bad = append(bad, unknown...)
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		return fmt.Errorf("unknown config paths: %v", bad)
	}
	return nil
}

// collectUnknownPaths 遞迴檢查 struct 與 slice 中的巢狀欄位，讓 Strict()
// 不會因為資料位於 list 元素內就放過未知設定。
func collectUnknownPaths(src any, dst reflect.Value, path string) ([]string, error) {
	if !dst.IsValid() {
		return nil, nil
	}
	dst = indirectSchemaValue(dst)
	if values, ok := src.([]any); ok {
		var bad []string
		switch dst.Kind() {
		case reflect.Slice:
			for i, value := range values {
				elem := reflect.New(dst.Type().Elem()).Elem()
				unknown, err := collectUnknownPaths(value, elem, indexPath(path, i))
				if err != nil {
					return nil, err
				}
				bad = append(bad, unknown...)
			}
		case reflect.Array:
			for i, value := range values {
				if i >= dst.Len() {
					break
				}
				unknown, err := collectUnknownPaths(value, dst.Index(i), indexPath(path, i))
				if err != nil {
					return nil, err
				}
				bad = append(bad, unknown...)
			}
		}
		return bad, nil
	}
	srcMap, ok := src.(map[string]any)
	if !ok {
		return nil, nil
	}
	if dst.Kind() != reflect.Struct {
		return nil, nil
	}
	fields := map[string]reflect.Value{}
	if err := mapConfigFields(dst, fields); err != nil {
		return nil, err
	}
	var bad []string
	for k, v := range srcMap {
		f, ok := fields[k]
		if !ok {
			bad = append(bad, joinPath(path, k))
			continue
		}
		unknown, err := collectUnknownPaths(v, f, joinPath(path, k))
		if err != nil {
			return nil, err
		}
		bad = append(bad, unknown...)
	}
	return bad, nil
}

// indirectSchemaValue 展開任意層 pointer；nil pointer 以其 element type 建立
// 暫時 schema value，讓 Strict() 不會因 runtime default 為 nil 而跳過欄位檢查。
func indirectSchemaValue(value reflect.Value) reflect.Value {
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			value = reflect.New(value.Type().Elem()).Elem()
			continue
		}
		value = value.Elem()
	}
	return value
}
