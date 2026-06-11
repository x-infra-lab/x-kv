package io.github.xinfra.lab.xkv.common.ratelimit;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * gRPC interceptor that rejects new requests with {@code UNAVAILABLE} when the
 * store is draining. Clients that receive this status will retry against another
 * store, achieving seamless traffic migration before shutdown.
 *
 * <p>Thread-safe: the draining flag is flipped once and never reset.
 */
public final class DrainingInterceptor implements ServerInterceptor {

    private final AtomicBoolean draining = new AtomicBoolean(false);

    public void startDraining() {
        draining.set(true);
    }

    public boolean isDraining() {
        return draining.get();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        if (draining.get()) {
            call.close(Status.UNAVAILABLE.withDescription("store is draining"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
