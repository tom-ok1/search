# Transport Layer & Serialization Framework

**Issue:** tom-ok1/search#51
**Date:** 2026-04-07

## Overview

Custom binary transport layer for inter-node communication, following Elasticsearch's transport architecture. This is the foundation for all distributed features (discovery, cluster state, distributed search, replication, recovery).

## Decisions

- **Wire protocol:** Custom binary over TCP (ES-faithful, not gRPC)
- **Executor model:** Named bounded worker pools (search, index, generic, cluster_state, transport_worker)
- **Connection model:** Multi-connection pools per node, categorized by type (REG, BULK, STATE, RECOVERY, PING)
- **Scope:** Full framework + Writeable for 3 representative actions (IndexDocument, GetDocument, Search)

## 1. Serialization Primitives

### Writeable Interface

```go
type Writeable interface {
    WriteTo(out *StreamOutput) error
}

type Reader[T any] func(in *StreamInput) (T, error)
```

Every transport message implements `Writeable`. `Reader` is the deserialization counterpart, registered per action.

### StreamOutput

Wraps `io.Writer`, provides typed write methods:

- `WriteByte`, `WriteBytes` -- raw bytes
- `WriteVInt`, `WriteVLong` -- variable-length encoding (zigzag, matching ES)
- `WriteString` -- VInt length prefix + UTF-8 bytes
- `WriteBool` -- single byte
- `WriteOptional[T Writeable]` -- 1-byte presence flag + value
- `WriteSlice[T Writeable]` -- VInt length prefix + elements
- `WriteByteArray` -- VInt length prefix + raw bytes (for `_source`, JSON blobs)
- `WriteGenericMap` -- type-tagged recursive map serialization (for query/aggs JSON)

### StreamInput

Wraps `io.Reader`, symmetric `Read*` methods for each `Write*`. Tracks bytes consumed for validation.

### Generic Map Serialization

For `map[string]any` (query JSON, aggregation JSON):

- Type tags: `0=nil, 1=string, 2=int64, 3=float64, 4=bool, 5=map, 6=slice`
- Recursive for nested structures
- `WriteGenericMap` / `ReadGenericMap` pair

## 2. Wire Protocol & Message Framing

### Frame Structure

```
Fixed Header (19 bytes):
  Marker: "ES" (2 bytes)
  MessageLength (4 bytes, big-endian)  -- total length after marker+length
  RequestID (8 bytes)
  Status (1 byte, bitfield)
  VariableHeaderLength (4 bytes)

Variable Header:
  Action name (string, requests only)
  ParentTaskID (string)

Payload:
  Writeable-serialized request or response
```

### Status Byte Flags

| Bit | Flag | Meaning |
|-----|------|---------|
| 0 | isRequest | 1 = request, 0 = response |
| 1 | isError | response carries serialized error |
| 2 | isHandshake | handshake message |
| 3 | isCompressed | payload is compressed (future, not Phase 1) |

### Key Types

```go
type Header struct {
    MessageLength       int32
    RequestID            int64
    Status               StatusFlags
    VariableHeaderLength int32
    Action               string
    ParentTaskID         string
}
```

### OutboundHandler

1. Serialize header (fixed + variable)
2. Serialize payload via `request.WriteTo(out)`
3. Write framed bytes to `net.Conn`

### InboundHandler

1. Read fixed header (19 bytes)
2. Read remaining bytes (message length - fixed header)
3. Parse variable header (action name)
4. Branch on status:
   - Request: look up handler by action, deserialize, dispatch to executor
   - Response: look up pending request by ID, deserialize, deliver to callback
   - Error: deserialize as `RemoteTransportError`, deliver to response handler

### InboundPipeline

Handles TCP stream reassembly:
- Accumulates bytes from `net.Conn` reads
- Detects message boundaries via length prefix
- Emits complete messages to `InboundHandler`

## 3. Connection Management

### Connection Types

```go
type ConnectionType int

const (
    ConnTypeREG      ConnectionType = iota  // general requests
    ConnTypeBULK                            // bulk indexing
    ConnTypeSTATE                           // cluster state publication
    ConnTypeRECOVERY                        // shard recovery
    ConnTypePING                            // keepalive
)
```

### ConnectionProfile

Configures per-type connection counts:

```go
type ConnectionProfile struct {
    ConnPerType      map[ConnectionType]int
    ConnectTimeout   time.Duration
    HandshakeTimeout time.Duration
    PingInterval     time.Duration
}
```

Defaults: `REG:6, BULK:3, STATE:1, RECOVERY:2, PING:1`.

### NodeConnection

Holds all connections to one remote node:

```go
type NodeConnection struct {
    node     DiscoveryNode
    channels map[ConnectionType][]net.Conn
    version  TransportVersion  // negotiated during handshake
    closed   atomic.Bool
}
```

- `SendRequest` selects connection by type, round-robins within the pool
- Implements `io.Closer`

### ConnectionManager

Node-level connection registry:

- `Connect(node)` -- opens all connections per profile, runs handshake, stores `NodeConnection`
- `GetConnection(nodeID)` -- cached lookup, returns `NodeNotConnectedError` if absent
- `DisconnectFromNode(nodeID)` -- closes and removes
- `ConnectedNodes()` -- lists all connected nodes

### Handshake Protocol

Runs on each new TCP connection:

1. Send `HandshakeRequest{Version: localVersion}` with handshake status flag
2. Receive `HandshakeResponse{Version: remoteVersion}`
3. Negotiated version = `min(local, remote)`
4. Incompatible versions: close connection, return error

### Local Node Optimization

`localNodeConnection` bypasses TCP entirely, calling handlers directly in-process. Matches ES's `TransportService.localNodeConnection`.

## 4. Request Dispatch & Handler Registry

### RequestHandlerRegistry

```go
type RequestHandlerRegistry[T Writeable] struct {
    Action   string
    Reader   Reader[T]
    Handler  TransportRequestHandler[T]
    Executor string
}
```

### Handler Interfaces

```go
type TransportRequestHandler[T any] interface {
    MessageReceived(request T, channel TransportChannel) error
}

type TransportChannel interface {
    SendResponse(response Writeable) error
    SendError(err error) error
}

type TransportResponseHandler[T any] interface {
    HandleResponse(response T)
    HandleError(err *RemoteTransportError)
    Reader() Reader[T]
    Executor() string
}
```

### TransportChannel Implementations

- `TcpTransportChannel` -- serializes response, writes to the TCP connection tagged with the original requestID
- `LocalTransportChannel` -- delivers directly to response handler for local node

### ResponseHandlers

Correlates outbound requests with response callbacks:

```go
type ResponseHandlers struct {
    nextRequestID atomic.Int64
    handlers      sync.Map  // requestID -> *ResponseContext
}

type ResponseContext struct {
    Handler   TransportResponseHandler
    Action    string
    NodeID    string
    CreatedAt time.Time
}
```

### Dispatch Flow -- Inbound Request

1. `InboundHandler` parses header, extracts action name
2. Looks up `RequestHandlerRegistry` by action
3. Deserializes request using `registry.Reader`
4. Submits to named executor: `threadPool.Get(registry.Executor).Execute(func() { handler.MessageReceived(request, channel) })`

### Dispatch Flow -- Inbound Response

1. Extract requestID from header
2. Look up and remove `ResponseContext` from `ResponseHandlers`
3. If error flag: deserialize as `RemoteTransportError`, call `handler.HandleError(err)`
4. Otherwise: deserialize via `handler.Reader()`, submit to `handler.Executor()`, call `handler.HandleResponse(response)`

### Timeout Handling

Background goroutine periodically scans `ResponseHandlers` for expired entries. On timeout: remove entry, call `handler.HandleError(ResponseTimeoutError)`.

## 5. Executor (Thread Pool) Abstraction

### Named Pools

| Pool | Purpose | Default Size |
|------|---------|-------------|
| `generic` | General-purpose fallback | `NumCPU * 4` |
| `search` | Query/fetch phase | `NumCPU + 1` |
| `index` | Index/delete/bulk | `NumCPU` |
| `transport_worker` | Inline on network goroutine | N/A (direct) |
| `cluster_state` | State publication/application | `1` |

### Core Types

```go
type Executor interface {
    Execute(task func()) error  // ErrRejected if full
    Shutdown()
}

type ThreadPool struct {
    pools map[string]Executor
}
```

### BoundedExecutor

Channel-based worker pool with fixed goroutine count reading from a buffered channel. `Execute()` submits to channel; returns `ErrRejected` if full (backpressure). `forceExecution` flag on handler registration allows bypassing the full check for critical operations.

### DirectExecutor

The `transport_worker` executor. Runs inline on calling goroutine. Used for lightweight handlers (ping, handshake).

## 6. TransportService

Top-level API wrapping everything.

```go
type TransportService struct {
    localNode         DiscoveryNode
    transport         *TcpTransport
    connectionManager *ConnectionManager
    requestHandlers   map[string]any  // action -> *RequestHandlerRegistry[T]
    responseHandlers  *ResponseHandlers
    threadPool        *ThreadPool
}
```

### Outbound API

```go
func (ts *TransportService) SendRequest(
    node DiscoveryNode,
    action string,
    request Writeable,
    options TransportRequestOptions,
    handler TransportResponseHandler,
) error
```

- If `node == localNode`: short-circuit via `sendLocalRequest()`, skip serialization
- Else: get connection, register response handler, send via `NodeConnection.SendRequest()`

### TransportRequestOptions

```go
type TransportRequestOptions struct {
    ConnType ConnectionType
    Timeout  time.Duration
}
```

### Inbound API

```go
func (ts *TransportService) RegisterRequestHandler(
    action string,
    executor string,
    reader Reader[T],
    handler TransportRequestHandler[T],
)
```

### Lifecycle

- `Start(address string)` -- starts TCP listener, accepts connections, starts inbound pipeline per connection, starts timeout reaper
- `Stop()` -- closes listener, drains connections, shuts down thread pools

### Error Types

```go
RemoteTransportError    // from remote node, carries action + nodeID + cause
NodeNotConnectedError   // target not in connection pool
ConnectTransportError   // connection establishment failed
SendRequestError        // connection broken mid-write
ResponseTimeoutError    // deadline exceeded
```

## 7. Representative Writeable Implementations

Three action pairs to prove the framework:

| Action | Why |
|--------|-----|
| IndexDocument | Write path: `_source` bytes, seqNo, CAS fields |
| GetDocument | Read path: optional fields (`Found`, `Source`) |
| Search | Complex: query JSON as `map[string]any`, variable-size hits |

Each request/response struct gets `WriteTo(*StreamOutput) error` and a corresponding `Read*(*StreamInput) (T, error)` function.

## 8. Integration with Node

Additive to the existing single-node architecture:

1. `Node` creates `ThreadPool` and `TransportService` at startup
2. `TransportService.Start()` listens on transport port (default `9300`, separate from HTTP `9200`)
3. Actions register handlers with `TransportService` using action names
4. Single-node mode: all calls go through `sendLocalRequest()` -- zero behavior change
5. Multi-node (future phases): actions call `TransportService.SendRequest()` to reach remote nodes

## 9. Package Structure

```
server/transport/
  stream.go              # StreamInput, StreamOutput
  writeable.go           # Writeable interface, Reader type
  protocol.go            # Header, StatusFlags, frame encoding/decoding
  pipeline.go            # InboundPipeline (TCP stream reassembly)
  handler.go             # InboundHandler, OutboundHandler
  channel.go             # TransportChannel interface + impls
  connection.go          # NodeConnection, ConnectionProfile, ConnectionType
  connection_manager.go  # ConnectionManager
  registry.go            # RequestHandlerRegistry, ResponseHandlers
  service.go             # TransportService
  tcp_transport.go       # TcpTransport (listener, accept, connect)
  handshake.go           # Handshake request/response/protocol
  executor.go            # ThreadPool, BoundedExecutor, DirectExecutor
  errors.go              # Transport error types
```

## 10. Minimal DiscoveryNode

A minimal `DiscoveryNode` struct is needed by the transport layer before Phase 2 (discovery) is implemented. Defined in `server/transport/` for now, to be moved to `server/cluster/` in Phase 2.

```go
type DiscoveryNode struct {
    ID      string  // unique node identifier
    Name    string  // human-readable name
    Address string  // host:port for transport
}
```

## ES Reference Files

- `server/src/main/java/org/elasticsearch/transport/Transport.java`
- `server/src/main/java/org/elasticsearch/transport/TransportService.java`
- `server/src/main/java/org/elasticsearch/transport/OutboundHandler.java`
- `server/src/main/java/org/elasticsearch/transport/InboundHandler.java`
- `server/src/main/java/org/elasticsearch/transport/TcpTransport.java`
- `server/src/main/java/org/elasticsearch/transport/TransportHandshaker.java`
- `server/src/main/java/org/elasticsearch/transport/ConnectionProfile.java`
- `server/src/main/java/org/elasticsearch/common/io/stream/StreamInput.java`
- `server/src/main/java/org/elasticsearch/common/io/stream/StreamOutput.java`
- `server/src/main/java/org/elasticsearch/common/io/stream/Writeable.java`
