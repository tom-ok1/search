package transport

// TransportVersion is the protocol version for wire compatibility.
const CurrentTransportVersion int32 = 1

// HandshakeRequest is sent when opening a new connection.
type HandshakeRequest struct {
	Version int32
}

func (r *HandshakeRequest) WriteTo(out *StreamOutput) error {
	return out.WriteVInt(r.Version)
}

func ReadHandshakeRequest(in *StreamInput) (*HandshakeRequest, error) {
	v, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	return &HandshakeRequest{Version: v}, nil
}

// HandshakeResponse is the reply to a HandshakeRequest.
type HandshakeResponse struct {
	Version int32
	NodeID  string
}

func (r *HandshakeResponse) WriteTo(out *StreamOutput) error {
	if err := out.WriteVInt(r.Version); err != nil {
		return err
	}
	return out.WriteString(r.NodeID)
}

func ReadHandshakeResponse(in *StreamInput) (*HandshakeResponse, error) {
	v, err := in.ReadVInt()
	if err != nil {
		return nil, err
	}
	nodeID, err := in.ReadString()
	if err != nil {
		return nil, err
	}
	return &HandshakeResponse{Version: v, NodeID: nodeID}, nil
}

// NegotiateVersion returns the minimum of two versions.
func NegotiateVersion(local, remote int32) int32 {
	if local < remote {
		return local
	}
	return remote
}
