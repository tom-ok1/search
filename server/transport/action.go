// server/transport/action.go
package transport

type ActionHandler interface {
	Name() string
}

type ActionRegistry struct {
	handlers map[string]ActionHandler
}

func NewActionRegistry() *ActionRegistry {
	return &ActionRegistry{
		handlers: make(map[string]ActionHandler),
	}
}

func (r *ActionRegistry) Register(handler ActionHandler) {
	r.handlers[handler.Name()] = handler
}

func (r *ActionRegistry) Get(name string) ActionHandler {
	return r.handlers[name]
}
