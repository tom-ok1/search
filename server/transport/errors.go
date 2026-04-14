package transport

import "fmt"

type RemoteTransportError struct {
	NodeID  string
	Action  string
	Message string
}

func (e *RemoteTransportError) Error() string {
	return fmt.Sprintf("[%s][%s] %s", e.NodeID, e.Action, e.Message)
}

func (e *RemoteTransportError) WriteTo(out *StreamOutput) error {
	if err := out.WriteString(e.NodeID); err != nil {
		return err
	}
	if err := out.WriteString(e.Action); err != nil {
		return err
	}
	return out.WriteString(e.Message)
}

func ReadRemoteTransportError(in *StreamInput) (*RemoteTransportError, error) {
	nodeID, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	action, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	msg, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &RemoteTransportError{NodeID: nodeID, Action: action, Message: msg}, nil
}

type NodeNotConnectedError struct {
	NodeID string
}

func (e *NodeNotConnectedError) Error() string {
	return fmt.Sprintf("node not connected [%s]", e.NodeID)
}

type ConnectTransportError struct {
	NodeID string
	Cause  error
}

func (e *ConnectTransportError) Error() string {
	return fmt.Sprintf("failed to connect to node [%s]: %v", e.NodeID, e.Cause)
}

func (e *ConnectTransportError) Unwrap() error {
	return e.Cause
}

type SendRequestError struct {
	Action string
	Cause  error
}

func (e *SendRequestError) Error() string {
	return fmt.Sprintf("failed to send [%s]: %v", e.Action, e.Cause)
}

func (e *SendRequestError) Unwrap() error {
	return e.Cause
}

type ResponseTimeoutError struct {
	Action    string
	RequestID int64
}

func (e *ResponseTimeoutError) Error() string {
	return fmt.Sprintf("response timeout [%s] requestID=%d", e.Action, e.RequestID)
}
