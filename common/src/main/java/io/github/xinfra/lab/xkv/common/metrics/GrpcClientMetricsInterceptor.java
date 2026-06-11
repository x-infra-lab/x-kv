package io.github.xinfra.lab.xkv.common.metrics;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public final class GrpcClientMetricsInterceptor implements ClientInterceptor {

    private final MeterRegistry registry;

    public GrpcClientMetricsInterceptor(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        String methodName = method.getFullMethodName();
        Timer.Sample sample = Timer.start(registry);

        return new ForwardingClientCall.SimpleForwardingClientCall<>(
                next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                var wrapped = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                        responseListener) {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        String statusCode = status.getCode().name();

                        sample.stop(Timer.builder("grpc_client_request_duration_seconds")
                                .tag("method", methodName)
                                .register(registry));

                        Counter.builder("grpc_client_requests_total")
                                .tag("method", methodName)
                                .tag("status", statusCode)
                                .register(registry)
                                .increment();

                        super.onClose(status, trailers);
                    }
                };
                super.start(wrapped, headers);
            }
        };
    }
}
