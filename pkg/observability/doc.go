// Package observability 提供 product 共用的 Health、Readiness 與 Prometheus
// Metrics HTTP endpoint。HTTP server 固定提供 GET /health（回應 ok）、GET /ready
// （readiness hook 完成前回應 503）與 GET /metrics。
//
// Module 會把 prometheus.Registerer 放入 framework DI。引用端可以在自己的
// module 中以 constructor 注入該 Registerer，使用 Register 建立並註冊自訂
// metrics；不應使用 global DefaultRegisterer 或會 panic 的 MustRegister。
//
// 外部 product 可直接以一般 framework provider 定義 metrics holder：
//
//	type Metrics struct {
//		Requests prometheus.Counter
//	}
//
//	func NewMetrics(registerer prometheus.Registerer) (*Metrics, error) {
//		requests := prometheus.NewCounter(prometheus.CounterOpts{
//			Name: "game_requests_total",
//			Help: "Total number of game requests.",
//		})
//		if err := registerer.Register(requests); err != nil {
//			return nil, fmt.Errorf("register game_requests_total: %w", err)
//		}
//		return &Metrics{Requests: requests}, nil
//	}
//
//	func Module(r framework.Registry) error {
//		return r.Provide(NewMetrics)
//	}
package observability
