package io.github.xinfra.lab.xkv.common.auth;

import io.grpc.Metadata;

public final class AuthConstants {

    private AuthConstants() {}

    public static final Metadata.Key<String> AUTH_TOKEN_KEY =
            Metadata.Key.of("x-auth-token", Metadata.ASCII_STRING_MARSHALLER);
}
