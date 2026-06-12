package io.github.xinfra.lab.xkv.common.metrics;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class GrpcServerMetricsInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerMetricsInterceptor.class);

    private final MeterRegistry registry;
    private final AtomicLong activeRequests;
    private final long slowThresholdMs;

    public GrpcServerMetricsInterceptor(MeterRegistry registry) {
        this(registry, 1000);
    }

    public GrpcServerMetricsInterceptor(MeterRegistry registry, long slowThresholdMs) {
        this.registry = registry;
        this.slowThresholdMs = slowThresholdMs;
        this.activeRequests = registry.gauge("grpc_server_active_requests",
                new AtomicLong(0));
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String method = call.getMethodDescriptor().getFullMethodName();
        Timer.Sample sample = Timer.start(registry);
        long startNanos = System.nanoTime();
        activeRequests.incrementAndGet();
        var decremented = new AtomicBoolean(false);

        var wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                if (decremented.compareAndSet(false, true)) {
                    activeRequests.decrementAndGet();
                }
                String statusCode = status.getCode().name();

                sample.stop(Timer.builder("grpc_server_request_duration_seconds")
                        .tag("method", method)
                        .register(registry));

                Counter.builder("grpc_server_requests_total")
                        .tag("method", method)
                        .tag("status", statusCode)
                        .register(registry)
                        .increment();

                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                if (elapsedMs >= slowThresholdMs) {
                    log.warn("[SLOW-LOG] method={} duration_ms={} status={}",
                            method, elapsedMs, statusCode);
                }

                super.close(status, trailers);
            }
        };

        var listener = next.startCall(wrappedCall, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onCancel() {
                if (decremented.compareAndSet(false, true)) {
                    activeRequests.decrementAndGet();
                }
                Counter.builder("grpc_server_requests_total")
                        .tag("method", method)
                        .tag("status", "CANCELLED")
                        .register(registry)
                        .increment();

                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                if (elapsedMs >= slowThresholdMs) {
                    log.warn("[SLOW-LOG] method={} duration_ms={} status=CANCELLED",
                            method, elapsedMs);
                }
                super.onCancel();
            }
        };
    }
}
