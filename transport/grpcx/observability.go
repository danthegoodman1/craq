package grpcx

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcObserver struct {
	logger       zerolog.Logger
	requests     *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	authFailures *prometheus.CounterVec
}

func newGRPCObserver(logger *zerolog.Logger, registry *prometheus.Registry) *grpcObserver {
	if logger == nil && registry == nil {
		return nil
	}
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	obs := &grpcObserver{
		logger: transportLoggerFromConfig(logger),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_grpc_requests_total",
			Help: "gRPC requests handled by plane and result code.",
		}, []string{"component", "method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_grpc_request_duration_seconds",
			Help:    "gRPC request duration by component and method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"component", "method"}),
		authFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_grpc_auth_failures_total",
			Help: "gRPC permission denied or unauthenticated failures.",
		}, []string{"component", "method", "code"}),
	}
	registry.MustRegister(obs.requests, obs.duration, obs.authFailures)
	return obs
}

func (o *grpcObserver) unaryInterceptor(component string) grpc.UnaryServerInterceptor {
	if o == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		o.observe(component, info.FullMethod, err, time.Since(start))
		return resp, err
	}
}

func (o *grpcObserver) streamInterceptor(component string) grpc.StreamServerInterceptor {
	if o == nil {
		return nil
	}
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		o.observe(component, info.FullMethod, err, time.Since(start))
		return err
	}
}

func (o *grpcObserver) observe(component string, method string, err error, dur time.Duration) {
	if o == nil {
		return
	}
	code := status.Code(err)
	codeStr := code.String()
	o.requests.WithLabelValues(component, method, codeStr).Inc()
	o.duration.WithLabelValues(component, method).Observe(dur.Seconds())
	if code == codes.PermissionDenied || code == codes.Unauthenticated {
		o.authFailures.WithLabelValues(component, method, codeStr).Inc()
	}
	if err != nil {
		level := zerolog.WarnLevel
		if code != codes.PermissionDenied && code != codes.Unauthenticated && code != codes.Canceled {
			level = zerolog.ErrorLevel
		}
		scoped := o.logger.With().
			Str("component", "grpc").
			Str("grpc_component", component).
			Str("grpc_method", method).
			Str("grpc_code", codeStr).
			Dur("duration", dur).
			Logger()
		scoped.WithLevel(level).
			AnErr("error", err).
			Msg("grpc request completed with error")
	}
}

func chainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	var filtered []grpc.UnaryServerInterceptor
	for _, interceptor := range interceptors {
		if interceptor != nil {
			filtered = append(filtered, interceptor)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		chained := handler
		for i := len(filtered) - 1; i >= 0; i-- {
			next := chained
			interceptor := filtered[i]
			chained = func(currentCtx context.Context, currentReq any) (any, error) {
				return interceptor(currentCtx, currentReq, info, next)
			}
		}
		return chained(ctx, req)
	}
}

func chainStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	var filtered []grpc.StreamServerInterceptor
	for _, interceptor := range interceptors {
		if interceptor != nil {
			filtered = append(filtered, interceptor)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		chained := handler
		for i := len(filtered) - 1; i >= 0; i-- {
			next := chained
			interceptor := filtered[i]
			chained = func(currentSrv any, currentStream grpc.ServerStream) error {
				return interceptor(currentSrv, currentStream, info, next)
			}
		}
		return chained(srv, stream)
	}
}
