package dispatcher

import "github.com/NeoJay0705/gaming-core-casino/pkg/framework"

// Module provides one shared Dispatcher to a product application.
func Module(r framework.Registry) error { return r.Provide(New) }

// Register creates a composition module that injects the shared Dispatcher
// and appends handlers to it. It deliberately does not add a lifecycle hook:
// handler registration is application wiring, not a running component.
func Register(registrations ...Registration) framework.Module {
	copyOf := append([]Registration(nil), registrations...)
	return func(r framework.Registry) error {
		for _, registration := range copyOf {
			registration := registration
			if err := r.Configure(func(dispatcher *Dispatcher) error {
				return dispatcher.Register(registration.Channel, registration.CommandID, registration.Handler)
			}); err != nil {
				return err
			}
		}
		return nil
	}
}
