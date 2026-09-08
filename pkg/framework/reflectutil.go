package framework

import (
	"reflect"
)

// IsNilDependency 檢查傳入的依賴介面或物件是否為 nil 或 Typed Nil。
//
// 詳細說明：
//   - 在 Go 的依賴注入 (DI) 與介面包裝中，將一個 nil 指標或實體傳給 interface{} 變數時，
//     該 interface{} 本身不為 nil，但底層包裝的值為 nil（即 Typed Nil）。
//   - 本函式透過 reflect.ValueOf 檢查 value 的 Kind 是否屬於可為 nil 的型別
//     （Chan, Func, Interface, Map, Pointer/Ptr, Slice），若屬於上述型別則調用 IsNil() 判斷。
//   - 適用於所有產品 SDK (gmsproduct, gameproduct, gateproduct) 的 DI 依賴校驗。
//
// 參數說明：
//   - value: 待檢查的依賴實體、介面或函式
//
// 返回值：
//   - bool: 若 value 為 nil 或底層指標/介面值為 nil 則傳回 true，否則傳回 false
//
// 範例：
//
//	var dependency any
//	if framework.IsNilDependency(dependency) {
//	    return fmt.Errorf("dependency is nil")
//	}
func IsNilDependency(value any) bool {
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
