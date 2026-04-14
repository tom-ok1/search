package transport

import "fmt"

// registerHandler is a test helper that creates a typed dispatch closure for request handlers.
func registerHandler[T any](
	m *RequestHandlerMap,
	action string,
	executor PoolName,
	reader Reader[T],
	handler func(request T, channel TransportChannel) error,
) {
	m.Register(&requestHandlerEntry{
		action:   action,
		executor: executor,
		dispatch: func(payload *StreamInput, channel TransportChannel) {
			req, err := reader(payload)
			if err != nil {
				channel.SendError(fmt.Errorf("deserialize request: %w", err))
				return
			}
			if err := handler(req, channel); err != nil {
				channel.SendError(err)
			}
		},
	})
}
