package io.github.xinfra.lab.xkv.common.auth;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class AuthServerInterceptor implements ServerInterceptor {

    private final String expectedToken;

    public AuthServerInterceptor(String expectedToken) {
        if (expectedToken == null || expectedToken.isEmpty()) {
            throw new IllegalArgumentException("auth token must not be empty");
        }
        this.expectedToken = expectedToken;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String token = headers.get(AuthConstants.AUTH_TOKEN_KEY);
        if (token == null || !MessageDigest.isEqual(
                expectedToken.getBytes(StandardCharsets.UTF_8),
                token.getBytes(StandardCharsets.UTF_8))) {
            call.close(Status.UNAUTHENTICATED
                    .withDescription("invalid or missing auth token"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
