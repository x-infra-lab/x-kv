package io.github.xinfra.lab.xkv.common.auth;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

public final class AuthClientInterceptor implements ClientInterceptor {

    private final String token;

    public AuthClientInterceptor(String token) {
        this.token = token;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<>(
                next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                if (token != null && !token.isEmpty()) {
                    headers.put(AuthConstants.AUTH_TOKEN_KEY, token);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
