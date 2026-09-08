// Package framework provides the dependency-injection application boundary.
package framework

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/dig"
)

// Phase 決定 Hook 的啟動順序；較早的 phase 先啟動、後停止。
type Phase int

const (
	PhaseInfrastructure Phase = 100
	PhaseService        Phase = 200
	PhaseIngress        Phase = 300
	PhaseReadiness      Phase = 400
)

// Hook 描述一個具有啟動／停止回呼的 lifecycle 元件。
type Hook struct {
	// Name 用於驗證唯一性及 lifecycle 錯誤訊息。
	Name string
	// Phase 決定此 Hook 相對於其他 Hook 的啟停順序。
	Phase Phase
	// OnStart 初始化元件；允許為 nil，表示只有停止邏輯。
	OnStart func(context.Context) error
	// OnStop 釋放元件；允許為 nil，表示只有啟動邏輯。
	OnStop func(context.Context) error
}

// ManagedResource is a DI dependency whose lifetime is owned by the framework
// once, and only once, its factory is actually resolved by an application hook.
type ManagedResource interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// Registry 提供依賴建構函式與 lifecycle Hook 建構函式的註冊介面。
type Registry interface {
	Provide(constructor any) error
	ProvideManaged(name string, phase Phase, constructor any) error
	AddHook(constructor any) error
}

// Module 在 App 建構期間註冊依賴與 lifecycle Hook。
type Module func(Registry) error

// ErrAppNotStartable 表示 one-shot App 已經啟動過，不能再次啟動。
var ErrAppNotStartable = errors.New("framework app is not startable")

type appLifecycleState uint8

const (
	appStateNew appLifecycleState = iota
	appStateStarting
	appStateStarted
	appStateStopping
	appStateStopped
	appStateFailed
)

func (s appLifecycleState) String() string {
	switch s {
	case appStateNew:
		return "new"
	case appStateStarting:
		return "starting"
	case appStateStarted:
		return "started"
	case appStateStopping:
		return "stopping"
	case appStateStopped:
		return "stopped"
	case appStateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// App 是由依賴圖與 lifecycle Hook 組成的 one-shot application boundary。
type App struct {
	lifecycleMu     sync.Mutex
	lifecycleState  appLifecycleState
	shutdownTimeout time.Duration
	hooks           []hookRegistration
	hookStarted     []int
	hookStopErr     error
}

type hookRegistration struct {
	Hook              Hook
	Constructor       string
	RegistrationIndex int
}

type registry struct {
	container         *dig.Container
	managed           *managedRegistry
	managedNames      map[string]struct{}
	registrationIndex int
}

type managedRegistration struct {
	Name              string
	Phase             Phase
	Value             ManagedResource
	Constructor       string
	RegistrationIndex int
}

type managedRegistry struct {
	mu     sync.Mutex
	values []managedRegistration
}

func (r *managedRegistry) Add(value managedRegistration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values = append(r.values, value)
}

func (r *managedRegistry) Values() []managedRegistration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]managedRegistration(nil), r.values...)
}

func (r *registry) Provide(constructor any) error { return r.container.Provide(constructor) }

// ProvideManaged registers a lazy DI factory for a resource controlled by the
// framework lifecycle. The factory is not invoked merely by registration: it
// joins the lifecycle only after an AddHook dependency actually resolves it.
func (r *registry) ProvideManaged(name string, phase Phase, constructor any) error {
	if strings.TrimSpace(name) == "" || name != strings.TrimSpace(name) {
		return fmt.Errorf("managed resource name is invalid: %q", name)
	}
	if name == "readiness" || phase == PhaseReadiness {
		return fmt.Errorf("managed resource %q cannot use readiness", name)
	}
	if !isValidPhase(phase) {
		return fmt.Errorf("managed resource %q has unknown phase %d", name, phase)
	}
	if _, exists := r.managedNames[name]; exists {
		return fmt.Errorf("duplicate managed resource %q", name)
	}
	value, err := validateManagedConstructor(constructor)
	if err != nil {
		return err
	}
	index := r.registrationIndex + 1
	wrapped := newManagedProvider(value, managedRegistration{
		Name:              name,
		Phase:             phase,
		Constructor:       runtimeFuncName(value),
		RegistrationIndex: index,
	}, r.managed)
	if err := r.container.Provide(wrapped); err != nil {
		return err
	}
	r.managedNames[name] = struct{}{}
	r.registrationIndex = index
	return nil
}

func (r *registry) AddHook(constructor any) error {
	value, err := validateHookConstructor(constructor)
	if err != nil {
		return err
	}
	index := r.registrationIndex + 1
	if err := r.container.Provide(newHookProvider(value, index)); err != nil {
		return err
	}
	r.registrationIndex = index
	return nil
}

func validateManagedConstructor(constructor any) (reflect.Value, error) {
	value := reflect.ValueOf(constructor)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return reflect.Value{}, errors.New("managed constructor must be a non-nil function")
	}
	t := value.Type()
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if t.IsVariadic() || (t.NumOut() != 1 && t.NumOut() != 2) {
		return reflect.Value{}, errors.New("managed constructor must return ManagedResource or (ManagedResource, error)")
	}
	if !t.Out(0).Implements(reflect.TypeOf((*ManagedResource)(nil)).Elem()) {
		return reflect.Value{}, fmt.Errorf("managed constructor first return %s does not implement ManagedResource", t.Out(0))
	}
	if t.NumOut() == 2 && !t.Out(1).Implements(errorType) {
		return reflect.Value{}, errors.New("managed constructor second return must implement error")
	}
	return value, nil
}

// newManagedProvider wraps a factory at the dig boundary. dig only executes
// the wrapper when a hook dependency requires its result, preserving lazy DI.
func newManagedProvider(value reflect.Value, registration managedRegistration, collector *managedRegistry) any {
	t := value.Type()
	inputs := make([]reflect.Type, t.NumIn())
	for i := range inputs {
		inputs[i] = t.In(i)
	}
	resourceType := t.Out(0)
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	wrapper := reflect.MakeFunc(reflect.FuncOf(inputs, []reflect.Type{resourceType, errorType}, false), func(args []reflect.Value) []reflect.Value {
		results := value.Call(args)
		if len(results) == 2 && !IsNilDependency(results[1].Interface()) {
			return []reflect.Value{results[0], results[1]}
		}
		resource, ok := results[0].Interface().(ManagedResource)
		if !ok || IsNilDependency(resource) {
			return []reflect.Value{
				reflect.Zero(resourceType),
				reflect.ValueOf(fmt.Errorf("managed resource %q constructor returned nil", registration.Name)),
			}
		}
		item := registration
		item.Value = resource
		collector.Add(item)
		return []reflect.Value{results[0], reflect.Zero(errorType)}
	})
	return wrapper.Interface()
}

func validateHookConstructor(constructor any) (reflect.Value, error) {
	value := reflect.ValueOf(constructor)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return reflect.Value{}, errors.New("hook constructor must be a non-nil function")
	}
	t := value.Type()
	hookType := reflect.TypeOf(Hook{})
	if t.IsVariadic() || (t.NumOut() != 1 && t.NumOut() != 2) || t.Out(0) != hookType {
		return reflect.Value{}, errors.New("hook constructor must return Hook or (Hook, error)")
	}
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if t.NumOut() == 2 && !t.Out(1).Implements(errorType) {
		return reflect.Value{}, errors.New("hook constructor second return must implement error")
	}
	return value, nil
}

// newHookProvider 將受限簽名的 Hook constructor 轉成 dig group provider。
// 這層反射只存在於 Registry 與 dig 的邊界，避免 lifecycle 執行期再解析 constructor。
func newHookProvider(value reflect.Value, index int) any {
	constructorName := runtimeFuncName(value)
	t := value.Type()
	outType := reflect.StructOf([]reflect.StructField{
		{Name: "Out", Type: reflect.TypeOf(dig.Out{}), Anonymous: true},
		{Name: "Value", Type: reflect.TypeOf(hookRegistration{}), Tag: `group:"framework.hook"`},
	})
	inputs := make([]reflect.Type, t.NumIn())
	for i := range inputs {
		inputs[i] = t.In(i)
	}
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	wrapper := reflect.MakeFunc(reflect.FuncOf(inputs, []reflect.Type{outType, errorType}, false), func(args []reflect.Value) []reflect.Value {
		results := value.Call(args)
		out := reflect.New(outType).Elem()
		if len(results) == 2 && !IsNilDependency(results[1].Interface()) {
			return []reflect.Value{out, results[1]}
		}
		item := hookRegistration{Hook: results[0].Interface().(Hook), Constructor: constructorName, RegistrationIndex: index}
		out.FieldByName("Value").Set(reflect.ValueOf(item))
		return []reflect.Value{out, reflect.Zero(errorType)}
	})
	return wrapper.Interface()
}

func runtimeFuncName(v reflect.Value) string {
	if fn := runtime.FuncForPC(v.Pointer()); fn != nil {
		return fn.Name()
	}
	return "<anonymous>"
}

// New 建立預設 graceful shutdown timeout 為 30 秒的 App。
func New(modules ...Module) (*App, error) { return newApp(30*time.Second, modules...) }

// NewWithShutdownTimeout 建立可自訂 graceful shutdown timeout 的 App。
func NewWithShutdownTimeout(timeout time.Duration, modules ...Module) (*App, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("shutdown timeout must be positive")
	}
	return newApp(timeout, modules...)
}

func newApp(timeout time.Duration, modules ...Module) (*App, error) {
	c := dig.New(dig.RecoverFromPanics())
	r := &registry{container: c, managed: &managedRegistry{}, managedNames: make(map[string]struct{})}
	for i, module := range modules {
		if module == nil {
			return nil, fmt.Errorf("module %d is nil", i)
		}
		if err := module(r); err != nil {
			return nil, fmt.Errorf("register module %d: %w", i, err)
		}
	}
	app := &App{shutdownTimeout: timeout}
	if err := c.Invoke(func(values struct {
		dig.In
		Hooks []hookRegistration `group:"framework.hook"`
	}) {
		app.hooks = append([]hookRegistration(nil), values.Hooks...)
	}); err != nil {
		return nil, fmt.Errorf("resolve framework hooks: %w", err)
	}
	for _, item := range r.managed.Values() {
		resource := item.Value
		app.hooks = append(app.hooks, hookRegistration{
			Hook: Hook{
				Name:    item.Name,
				Phase:   item.Phase,
				OnStart: resource.Start,
				OnStop:  resource.Stop,
			},
			Constructor:       item.Constructor,
			RegistrationIndex: item.RegistrationIndex,
		})
	}
	if err := validateHooks(app.hooks); err != nil {
		return nil, err
	}
	sort.SliceStable(app.hooks, func(i, j int) bool {
		if app.hooks[i].Hook.Phase != app.hooks[j].Hook.Phase {
			return app.hooks[i].Hook.Phase < app.hooks[j].Hook.Phase
		}
		return app.hooks[i].RegistrationIndex < app.hooks[j].RegistrationIndex
	})
	return app, nil
}

func isValidPhase(phase Phase) bool {
	switch phase {
	case PhaseInfrastructure, PhaseService, PhaseIngress, PhaseReadiness:
		return true
	default:
		return false
	}
}

func validateHooks(hooks []hookRegistration) error {
	names := make(map[string]hookRegistration, len(hooks))
	readiness := 0
	for _, item := range hooks {
		name := item.Hook.Name
		if name == "" {
			return fmt.Errorf("invalid hook %s: name is empty", item.Constructor)
		}
		if name != strings.TrimSpace(name) {
			return fmt.Errorf("invalid hook %s: name has surrounding whitespace", item.Constructor)
		}
		if !isValidPhase(item.Hook.Phase) {
			return fmt.Errorf("invalid hook %q: unknown phase %d", name, item.Hook.Phase)
		}
		if item.Hook.OnStart == nil && item.Hook.OnStop == nil {
			return fmt.Errorf("invalid hook %q: both callbacks are nil", name)
		}
		if previous, ok := names[name]; ok {
			return fmt.Errorf("duplicate hook %q: constructors %s and %s", name, previous.Constructor, item.Constructor)
		}
		names[name] = item
		if name == "readiness" {
			readiness++
			if item.Hook.Phase != PhaseReadiness {
				return fmt.Errorf("invalid hook %q: must use readiness phase", name)
			}
		}
		if item.Hook.Phase == PhaseReadiness && name != "readiness" {
			return fmt.Errorf("invalid hook %q: only readiness may use readiness phase", name)
		}
	}
	if readiness != 1 {
		return fmt.Errorf("invalid hook graph: expected exactly one readiness hook, got %d", readiness)
	}
	return nil
}

// Start 依 phase 順序執行 Hook。App 是 one-shot：Start 成功或失敗後都不能重試。
// 若後續 Hook 啟動失敗，只反向回滾已成功啟動的 Hook。
//
// lifecycle mutex 刻意涵蓋 callback 執行期間，確保 Start 與 Stop 不會交錯。
// Hook callback 不可同步呼叫同一個 App 的 Start 或 Stop，否則會造成 deadlock。
func (a *App) Start(ctx context.Context) error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.lifecycleState != appStateNew {
		return fmt.Errorf("%w: current state=%s", ErrAppNotStartable, a.lifecycleState)
	}
	a.lifecycleState = appStateStarting
	for i := range a.hooks {
		item := &a.hooks[i]
		if item.Hook.OnStart != nil {
			if err := item.Hook.OnStart(ctx); err != nil {
				rollbackCtx, cancel := a.newShutdownContext(ctx)
				rollback := a.stopStartedHooks(rollbackCtx)
				cancel()
				a.lifecycleState = appStateFailed
				return errors.Join(fmt.Errorf("start hook %q (phase %d): %w", item.Hook.Name, item.Hook.Phase, err), rollback)
			}
		}
		a.hookStarted = append(a.hookStarted, i)
	}
	a.lifecycleState = appStateStarted
	return nil
}

// Stop 具冪等性。Start 前呼叫是 no-op；lifecycle 成功或失敗後再次呼叫時，
// 會回傳先前彙整的停止錯誤。
func (a *App) Stop(ctx context.Context) error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	switch a.lifecycleState {
	case appStateNew:
		return nil
	case appStateFailed, appStateStopped:
		return a.hookStopErr
	case appStateStarted:
		a.lifecycleState = appStateStopping
		err := a.stopStartedHooks(ctx)
		a.lifecycleState = appStateStopped
		return err
	default:
		// starting／stopping 在 lifecycleMu 序列化下觀察不到：並發的 Stop 會阻塞到
		// 整段 Start／Stop 結束才取得鎖。保留這個防禦性分支是為了萬一未來縮小鎖範圍，
		// 未處理的狀態要明確回錯，而不是靜默跳過 Hook 的停止。
		return fmt.Errorf("framework app is not stoppable: current state=%s", a.lifecycleState)
	}
}

func (a *App) stopStartedHooks(ctx context.Context) error {
	// 呼叫者必須持有 lifecycleMu；停止流程即使遇到錯誤仍會繼續處理其餘 Hook，
	// 最後以 errors.Join 回傳所有停止錯誤。
	if len(a.hookStarted) == 0 {
		return a.hookStopErr
	}
	var errs []error
	for i := len(a.hookStarted) - 1; i >= 0; i-- {
		item := &a.hooks[a.hookStarted[i]]
		if item.Hook.OnStop != nil {
			if err := item.Hook.OnStop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("stop hook %q (phase %d): %w", item.Hook.Name, item.Hook.Phase, err))
			}
		}
	}
	a.hookStarted = nil
	a.hookStopErr = errors.Join(errs...)
	return a.hookStopErr
}

func (a *App) newShutdownContext(parent context.Context) (context.Context, context.CancelFunc) {
	// rollback／shutdown 不應被已取消的 startup context 中斷，但仍保留 parent 的 values。
	return context.WithTimeout(context.WithoutCancel(parent), a.shutdownTimeout)
}

// Run 啟動 App，等待 context 取消或 SIGINT／SIGTERM，接著以設定的 timeout 執行
// graceful shutdown。
func (a *App) Run(ctx context.Context) error {
	// 必須在啟動前建立 signal handler，才能記住慢速啟動期間收到的終止訊號，
	// 並在啟動完成後執行 graceful shutdown。
	signalCtx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	if err := a.Start(signalCtx); err != nil {
		return err
	}
	<-signalCtx.Done()
	shutdownCtx, cancel := a.newShutdownContext(ctx)
	defer cancel()
	return a.Stop(shutdownCtx)
}
