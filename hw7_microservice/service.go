package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

func StartMyMicroservice(ctx context.Context, listenAddr string, aclData string) error {
	acl := make(map[string][]string)
	err := json.Unmarshal([]byte(aclData), &acl)
	if err != nil {
		return fmt.Errorf("could not parce ACL data: %v", err)
	}

	lis, err := net.Listen("tcp", listenAddr)

	as := newAdminServer()
	mid := middleware{
		acl:  acl,
		log:  as.log,
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(mid.unaryInterceptor),
		grpc.StreamInterceptor(mid.streamInterceptor),
	)

	RegisterAdminServer(server, as)
	RegisterBizServer(server, bizServer{})

	go func() {
		err := server.Serve(lis)
		if err != nil && err != grpc.ErrServerStopped {
			log.Println(err)
		}
	}()

	go func() {
		<-ctx.Done()
		as.stop()
		server.GracefulStop()
	}()

	return nil
}

type middleware struct {
	acl  map[string][]string
	log  func(Event)
}

func (mid *middleware) streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	err := mid.process(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return handler(srv, ss)
}

func (mid *middleware) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	err := mid.process(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func (mid *middleware) process(ctx context.Context, method string) error {
	consumer, err := getConsumer(ctx)
	if err != nil {
		return err
	}

	host, err := getClientHost(ctx)
	if err != nil {
		log.Println(err)
		return err
	}

	mid.logRequest(consumer, method, host)
	err = mid.checkAuth(consumer, method, mid.acl)
	if err != nil {
		return err
	}
	return nil
}

func getConsumer(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Errorf(codes.Unauthenticated,
			"could not get metadata from incoming context")
	}

	consumer := md.Get("consumer")
	if len(consumer) == 0 {
		return "", status.Errorf(codes.Unauthenticated,
			"could not get consumer from metadata")
	}

	return consumer[0], nil
}

func getClientHost(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("could not get client metadata from incoming context")
	}

	authority, ok := md[":authority"]
	if !ok || len(authority) == 0 {
		return "", errors.New("could not get client host from incoming context")
	}

	return strings.Split(authority[0], ":")[0] + ":", nil
}

func (mid *middleware) logRequest(consumer string, method string, host string) {
	mid.log(Event{
		Timestamp: time.Now().Unix(),
		Consumer:  consumer,
		Method:    method,
		Host:      host,
	})
}

func (mid *middleware) checkAuth(consumer string, method string, acl map[string][]string) error {
	methods, ok := acl[consumer]
	if !ok {
		return status.Errorf(codes.Unauthenticated,
			"unknown consumer")
	}

	for _, allowed := range methods {
		if strings.Contains(
			method,
			strings.TrimSuffix(allowed, "*"),
		) {
			return nil
		}
	}

	return status.Errorf(codes.Unauthenticated,
		"method %v is not allowed for %v", method, consumer[0])
}

type adminServer struct {
	sync.Mutex
	subs   map[int]chan Event
	nextID int
}

func (as *adminServer) newSub() (int, <-chan Event) {
	as.Lock()
	defer as.Unlock()

	newSub := make(chan Event)
	id := as.nextID
	as.subs[id] = newSub
	as.nextID++

	return id, newSub
}

func (as *adminServer) deleteSub(id int) {
	as.Lock()
	defer as.Unlock()

	delete(as.subs, id)
}

func (as *adminServer) log(e Event) {
	as.Lock()
	defer as.Unlock()

	for _, sub := range as.subs {
		sub <- e
	}
}

func newAdminServer() *adminServer {
	as := adminServer{
		subs: make(map[int]chan Event),
	}
	return &as
}

func (as *adminServer) stop() {
	as.Lock()
	defer as.Unlock()

	for _, sub := range as.subs {
		close(sub)
	}
}

func (as *adminServer) Logging(in *Nothing, serv Admin_LoggingServer) error {
	id, events := as.newSub()
	defer func() { as.deleteSub(id) }()
	for event := range events {
		err := serv.Send(&event)
		if err != nil {
			return err
		}
	}
	return nil
}

func (as *adminServer) Statistics(interval *StatInterval, serv Admin_StatisticsServer) error {
	statByConsumer := make(map[string]uint64)
	statByMethod := make(map[string]uint64)
	id, sub := as.newSub()
	defer func() { as.deleteSub(id) }()

	ticker := time.NewTicker(time.Duration(interval.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event := <-sub:
			statByConsumer[event.Consumer]++
			statByMethod[event.Method]++
		case <-ticker.C:
			err := serv.Send(&Stat{
				Timestamp:  time.Now().Unix(),
				ByConsumer: statByConsumer,
				ByMethod:   statByMethod,
			})
			if err != nil {
				return err
			}
			statByConsumer = make(map[string]uint64)
			statByMethod = make(map[string]uint64)
		}
	}
}

type bizServer struct{}

func (bizServer) Check(context.Context, *Nothing) (*Nothing, error) {
	return new(Nothing), nil
}

func (bizServer) Add(context.Context, *Nothing) (*Nothing, error) {
	return new(Nothing), nil
}

func (bizServer) Test(context.Context, *Nothing) (*Nothing, error) {
	return new(Nothing), nil
}
