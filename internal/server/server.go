package server

import (
	"context"
	"fmt"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	api "github.com/izenberk/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	// OpenTelemetry (replaces OpenCensus)
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	// Zap (unchanged)
	"go.uber.org/zap"
)

type Config struct {
	CommitLog 	CommitLog
	Authorizer 	Authorizer
}

const (
	objectWildcard 	= "*"
	produceAction 	= "produce"
	consumeAction		= "consume"
)

var _ api.LogServer = (*grpcServer)(nil)

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (
	*grpc.Server,
	error,
) {
	logger := zap.L().Named("server")

	logTraceID := func(ctx context.Context) logging.Fields {
		if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
			return logging.Fields{
				"trace_id", span.TraceID().String(),
				"span_id", span.SpanID().String(),
			}
		}
		return nil
	}

	logOtps := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithFieldsFromContext(logTraceID),
		logging.WithDurationField(func (duration time.Duration) logging.Fields {
			return logging.Fields{
				"grpc.time_ns", duration.Nanoseconds(),
			}
		}),
	}


	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)

		otel.SetTracerProvider(tp)
	}


	opts = append(opts, grpc.ChainStreamInterceptor(
			logging.StreamServerInterceptor(InterceptorLogger(logger), logOtps...),
			grpc_auth.StreamServerInterceptor(authenticate),
		),
		grpc.ChainUnaryInterceptor(
			logging.UnaryServerInterceptor(InterceptorLogger(logger), logOtps...),
			grpc_auth.UnaryServerInterceptor(authenticate),
		),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
		if err := s.Authorizer.Authorize(
			subject(ctx),
			objectWildcard,
			produceAction,
		); err != nil {
			return nil, err
		}
		offset, err := s.CommitLog.Append(req.Record)
		if err != nil {
			return nil, err
		}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
		if err := s.Authorizer.Authorize(
			subject(ctx),
			objectWildcard,
			consumeAction,
		); err != nil {
			return nil, err
		}
		record, err := s.CommitLog.Read(req.Offset)
		if err != nil {
			return nil, err
		}
		return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(
	stream api.Log_ProduceStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't fine peer info",
		).Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

func InterceptorLogger(l *zap.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)/2)

		for i := 0; i < len(fields); i += 2 {
			key := fields[i].(string)
			value := fields[i+1]

			switch v := value.(type) {
			case string:
				f = append(f, zap.String(key, v))
			case int:
				f = append(f, zap.Int(key, v))
			case int64:
				f = append(f, zap.Int64(key, v)) // Catches duration.Nanoseconds()!
			case bool:
				f = append(f, zap.Bool(key, v))
			default:
				f = append(f, zap.Any(key, v))
			}
		}

		logger := l.WithOptions(zap.AddCallerSkip(1)).With(f...)

		switch lvl {
		case logging.LevelDebug:
			logger.Debug(msg)
		case logging.LevelInfo:
			logger.Info(msg)
		case logging.LevelWarn:
			logger.Warn(msg)
		case logging.LevelError:
			logger.Error(msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}