package io.github.xinfra.lab.xkv.kv.ratelimit;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public final class ResourceGroupInterceptor implements ServerInterceptor {

    public static final Metadata.Key<String> RESOURCE_GROUP_KEY =
            Metadata.Key.of("x-resource-group", Metadata.ASCII_STRING_MARSHALLER);

    private final ResourceGroupThrottler throttler;
    private final long costPerRequest;

    public ResourceGroupInterceptor(ResourceGroupThrottler throttler) {
        this(throttler, 1);
    }

    public ResourceGroupInterceptor(ResourceGroupThrottler throttler, long costPerRequest) {
        this.throttler = throttler;
        this.costPerRequest = costPerRequest;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String group = headers.get(RESOURCE_GROUP_KEY);
        if (group == null || group.isEmpty()) {
            group = ResourceGroupThrottler.DEFAULT_GROUP;
        }

        if (!throttler.tryConsume(group, costPerRequest)) {
            call.close(Status.RESOURCE_EXHAUSTED
                    .withDescription("resource group '" + group + "' rate limit exceeded"),
                    new Metadata());
            return new ServerCall.Listener<>() {};
        }

        return next.startCall(call, headers);
    }
}
