package server

import (
	"context"
	"strings"
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
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	// Zap (unchanged)
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	zapOtps := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithDurationField(func (duration time.Duration) logging.Fields {
			return logging.Fields{
				"grpc.time_ns", duration.Nanoseconds(),
			}
		}),
	}

	customSampler := NewProducePrioritySampler(0.5)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	mp := sdkmetric.New

	opts = append(opts, grpc.StreamInterceptor(
			grpc_auth.StreamServerInterceptor(authenticate),
		),
		grpc.ChainUnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(authenticate),
		),
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

type ProducePrioritySampler struct {
	halfSampler sdktrace.Sampler
}

func NewProducePrioritySampler(ratio float64) sdktrace.Sampler {
	return &ProducePrioritySampler{
		halfSampler: sdktrace.TraceIDRatioBased(ratio),
	}
}

func (s *ProducePrioritySampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if strings.Contains(p.Name, "Produce") {
		return sdktrace.SamplingResult{
			Decision: sdktrace.RecordAndSample,
		}
	}

	return s.halfSampler.ShouldSample(p)
}

func (s *ProducePrioritySampler) Description() string {
	return "ProducePrioritySampler{Produce=1.0, Others=0.5}"
}