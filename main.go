package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
	"unicode/utf8"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/golang/protobuf/jsonpb"
	oldproto "github.com/golang/protobuf/proto" // Still used in etcd
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	// non-deprecated proto
)

// loggingUnaryInterceptor intercepts unary RPCs to log the decoded protobuf request and response.
func loggingUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	startTime := time.Now()
	resp, err := handler(ctx, req)
	duration := time.Since(startTime)

	logEntry := LogEntry{
		Timestamp: startTime.Format(time.RFC3339Nano),
		Method:    info.FullMethod,
		Type:      "unary",
		Duration:  duration.String(),
		Request:   marshalProtoMessage(req),
		Response:  marshalProtoMessage(resp),
		Error:     errorToString(err),
		// Metadata:  metadataFromContext(ctx),
	}

	outputLogEntry(logEntry)
	return resp, err
}

// loggingStreamInterceptor intercepts streaming RPCs to log the decoded protobuf messages.
func loggingStreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	startTime := time.Now()
	err := handler(srv, &loggingStreamServer{ServerStream: ss, methodName: info.FullMethod})
	duration := time.Since(startTime)

	logEntry := LogEntry{
		Timestamp: startTime.Format(time.RFC3339Nano),
		Method:    info.FullMethod,
		Type:      "stream",
		Duration:  duration.String(),
		Error:     errorToString(err),
	}

	outputLogEntry(logEntry)
	return err
}

// loggingStreamServer intercepts streaming RPCs to log the decoded protobuf messages.
type loggingStreamServer struct {
	grpc.ServerStream
	methodName string
}

func (s *loggingStreamServer) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	logEntry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Method:    s.methodName,
		Type:      "stream_recv",
		Message:   marshalProtoMessage(m),
		Error:     errorToString(err),
	}
	outputLogEntry(logEntry)
	return err
}

func (s *loggingStreamServer) SendMsg(m interface{}) error {
	err := s.ServerStream.SendMsg(m)
	logEntry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Method:    s.methodName,
		Type:      "stream_send",
		Message:   marshalProtoMessage(m),
		Error:     errorToString(err),
	}
	outputLogEntry(logEntry)
	return err
}

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp string            `json:"timestamp"`
	Method    string            `json:"method"`
	Type      string            `json:"type"`
	Duration  string            `json:"duration,omitempty"`
	Request   json.RawMessage   `json:"request,omitempty"`
	Response  json.RawMessage   `json:"response,omitempty"`
	Message   json.RawMessage   `json:"message,omitempty"`
	Error     string            `json:"error,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// outputLogEntry marshals a LogEntry to JSON and outputs it
func outputLogEntry(entry LogEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal log entry: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

// marshalProtoMessage converts a proto.Message to a JSON-formatted byte slice.
// It decodes base64-encoded bytes fields to UTF-8 strings for readability.
func marshalProtoMessage(m interface{}) json.RawMessage {
	if msg, ok := m.(oldproto.Message); ok {
		marshaller := &jsonpb.Marshaler{}
		str, err := marshaller.MarshalToString(msg)
		if err != nil {
			return json.RawMessage(`"<error marshaling message>"`)
		}
		// Unmarshal to map[string]interface{}
		var data interface{}
		if err := json.Unmarshal([]byte(str), &data); err != nil {
			return json.RawMessage(`"<error unmarshaling JSON>"`)
		}
		// Decode base64-encoded strings
		data = decodeBase64InMap(data)
		// Marshal back to JSON
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return json.RawMessage(`"<error marshaling JSON>"`)
		}
		return json.RawMessage(jsonBytes)
	}
	return nil
}

// decodeBase64InMap recursively traverses the data and decodes base64 strings.
func decodeBase64InMap(data interface{}) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		for key, val := range v {
			v[key] = decodeBase64InMap(val)
		}
		return v
	case []interface{}:
		for i, val := range v {
			v[i] = decodeBase64InMap(val)
		}
		return v
	case string:
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err == nil && utf8.Valid(decoded) {
			return string(decoded)
		}
		return v
	default:
		return v
	}
}

func errorToString(err error) string {
	if err != nil && err != io.EOF {
		return err.Error()
	}
	return ""
}

func main() {
	listenAddr := flag.String("listen", ":50051", "Address to listen on")
	etcdAddr := flag.String("etcd", "localhost:2379", "Address of etcd server")
	flag.Parse()

	// Create etcd client configuration
	etcdConfig := clientv3.Config{
		Endpoints:   []string{*etcdAddr},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}

	// Create etcd client
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	// Get the underlying gRPC connection
	etcdConn := etcdClient.ActiveConnection()

	// Create a listener
	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create a gRPC server with interceptors
	s := grpc.NewServer(
		grpc.UnaryInterceptor(loggingUnaryInterceptor),
		grpc.StreamInterceptor(loggingStreamInterceptor),
	)

	// Register etcd services to proxy
	registerEtcdServices(s, etcdConn)

	log.Printf("Proxy server listening on %s, forwarding to etcd at %s", *listenAddr, *etcdAddr)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// registerEtcdServices registers the etcd services to the proxy server
func registerEtcdServices(s *grpc.Server, conn *grpc.ClientConn) {
	etcdserverpb.RegisterKVServer(s, &kvServerProxy{client: etcdserverpb.NewKVClient(conn)})
	etcdserverpb.RegisterWatchServer(s, &watchServerProxy{client: etcdserverpb.NewWatchClient(conn)})
	etcdserverpb.RegisterLeaseServer(s, &leaseServerProxy{client: etcdserverpb.NewLeaseClient(conn)})
	/*
		// Register Cluster service
		etcdserverpb.RegisterClusterServer(s, etcdserverpb.NewClusterClient(conn))
		// Register Maintenance service
		etcdserverpb.RegisterMaintenanceServer(s, etcdserverpb.NewMaintenanceClient(conn))
		// Register Auth service
		etcdserverpb.RegisterAuthServer(s, etcdserverpb.NewAuthClient(conn))
	*/
}

// kvServerProxy implements etcdserverpb.KVServer and forwards requests to a KVClient
type kvServerProxy struct {
	client etcdserverpb.KVClient
}

func (p *kvServerProxy) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return p.client.Range(ctx, req)
}

func (p *kvServerProxy) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return p.client.Put(ctx, req)
}

func (p *kvServerProxy) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return p.client.DeleteRange(ctx, req)
}

func (p *kvServerProxy) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return p.client.Txn(ctx, req)
}

func (p *kvServerProxy) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return p.client.Compact(ctx, req)
}

type watchServerProxy struct {
	client etcdserverpb.WatchClient
}

func (p *watchServerProxy) Watch(srv etcdserverpb.Watch_WatchServer) error {
	// Create a client stream
	clientStream, err := p.client.Watch(srv.Context())
	if err != nil {
		return err
	}

	// Start goroutines to proxy messages between client and server streams
	errCh := make(chan error, 2)

	// Client to Server
	go func() {
		for {
			req, err := srv.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := clientStream.Send(req); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Server to Client
	go func() {
		for {
			resp, err := clientStream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := srv.Send(resp); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Wait for error
	return <-errCh
}

// leaseServerProxy implements etcdserverpb.LeaseServer
type leaseServerProxy struct {
	client etcdserverpb.LeaseClient
}

// LeaseGrant forwards the LeaseGrant RPC
func (p *leaseServerProxy) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return p.client.LeaseGrant(ctx, req)
}

// LeaseRevoke forwards the LeaseRevoke RPC
func (p *leaseServerProxy) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return p.client.LeaseRevoke(ctx, req)
}

// LeaseKeepAlive handles the bidirectional streaming RPC
func (p *leaseServerProxy) LeaseKeepAlive(srv etcdserverpb.Lease_LeaseKeepAliveServer) error {
	clientStream, err := p.client.LeaseKeepAlive(srv.Context())
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)

	// Client to Server
	go func() {
		for {
			req, err := srv.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := clientStream.Send(req); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Server to Client
	go func() {
		for {
			resp, err := clientStream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := srv.Send(resp); err != nil {
				errCh <- err
				return
			}
		}
	}()

	return <-errCh
}

// LeaseTimeToLive forwards the LeaseTimeToLive RPC
func (p *leaseServerProxy) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return p.client.LeaseTimeToLive(ctx, req)
}

// LeaseLeases forwards the LeaseLeases RPC
func (p *leaseServerProxy) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return p.client.LeaseLeases(ctx, req)
}
