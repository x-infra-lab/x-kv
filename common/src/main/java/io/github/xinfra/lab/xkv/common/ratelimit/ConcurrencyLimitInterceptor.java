package io.github.xinfra.lab.xkv.common.ratelimit;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.concurrent.Semaphore;

public final class ConcurrencyLimitInterceptor implements ServerInterceptor {

    private final Semaphore semaphore;

    public ConcurrencyLimitInterceptor(int maxConcurrent) {
        this.semaphore = new Semaphore(maxConcurrent);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        if (!semaphore.tryAcquire()) {
            call.close(Status.RESOURCE_EXHAUSTED
                    .withDescription("server is busy"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        var listener = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            private volatile boolean released;

            private void release() {
                if (!released) {
                    released = true;
                    semaphore.release();
                }
            }

            @Override
            public void onCancel() {
                release();
                super.onCancel();
            }

            @Override
            public void onComplete() {
                release();
                super.onComplete();
            }

            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (Throwable t) {
                    release();
                    throw t;
                }
            }
        };
    }
}
