package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	oldproto "github.com/golang/protobuf/proto" // Still used in etcd
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/prototext"
	newproto "google.golang.org/protobuf/proto" // non-deprecated proto
)

// loggingUnaryInterceptor intercepts unary RPCs to log the decoded protobuf request and response.
func loggingUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	log.Printf("Unary request - Method:%s\nRequest:\n%s", info.FullMethod, marshalProtoMessage(req))

	// Log incoming metadata if needed
	/*
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			log.Printf("Incoming metadata:\n%v", md)
		}
	*/

	resp, err := handler(ctx, req)
	if err != nil {
		log.Printf("Unary error - Method:%s\nError:%v", info.FullMethod, err)
	} else {
		log.Printf("Unary response - Method:%s\nResponse:\n%s", info.FullMethod, marshalProtoMessage(resp))
	}
	return resp, err
}

// loggingStreamServer intercepts streaming RPCs to log the decoded protobuf messages.
type loggingStreamServer struct {
	grpc.ServerStream
	methodName string
}

func (s *loggingStreamServer) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		log.Printf("Stream recv - Method:%s\nMessage Type:%s\nMessage:\n%s", s.methodName, reflect.TypeOf(m), marshalProtoMessage(m))
	} else if err == io.EOF {
		log.Printf("Stream recv - Method:%s - EOF", s.methodName)
	} else {
		log.Printf("Stream recv error - Method:%s\nError:%v", s.methodName, err)
	}
	return err
}

func (s *loggingStreamServer) SendMsg(m interface{}) error {
	log.Printf("Stream send - Method:%s\nMessage Type:%s\nMessage:\n%s", s.methodName, reflect.TypeOf(m), marshalProtoMessage(m))
	return s.ServerStream.SendMsg(m)
}

func loggingStreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	log.Printf("Stream started - Method:%s", info.FullMethod)
	err := handler(srv, &loggingStreamServer{ServerStream: ss, methodName: info.FullMethod})
	if err != nil {
		log.Printf("Stream error - Method:%s\nError:%v", info.FullMethod, err)
	} else {
		log.Printf("Stream finished - Method:%s", info.FullMethod)
	}
	return err
}

// marshalProtoMessage converts a proto.Message to a human-readable string.
func marshalProtoMessage(m interface{}) string {
	switch msg := m.(type) {
	case oldproto.Message:
		return oldproto.MarshalTextString(msg)
	case newproto.Message:
		return prototext.Format(msg)
	default:
		return fmt.Sprintf("<not a proto.Message: %v>", m)
	}
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
	// Register KV service
	etcdserverpb.RegisterKVServer(s, &kvServerProxy{client: etcdserverpb.NewKVClient(conn)})
	// Register Watch service
	etcdserverpb.RegisterWatchServer(s, &watchServerProxy{client: etcdserverpb.NewWatchClient(conn)})

	/*
		// Register Lease service
		etcdserverpb.RegisterLeaseServer(s, etcdserverpb.NewLeaseClient(conn))
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
