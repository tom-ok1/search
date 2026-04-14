package transport

// Writeable is implemented by all types that can be serialized to the transport wire format.
type Writeable interface {
	WriteTo(out *StreamOutput) error
}

// Reader is a function that deserializes a value from a StreamInput.
type Reader[T any] func(in *StreamInput) (T, error)
