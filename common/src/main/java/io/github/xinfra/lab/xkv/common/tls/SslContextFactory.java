package io.github.xinfra.lab.xkv.common.tls;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;

public final class SslContextFactory {

    private SslContextFactory() {}

    public static SslContext forServer(TlsConfig cfg) throws SSLException {
        if (cfg == null) return null;
        File cert = cfg.certChain().toFile();
        File key = cfg.privateKey().toFile();
        var builder = SslContextBuilder.forServer(cert, key);
        if (cfg.trustCerts() != null) {
            builder.trustManager(cfg.trustCerts().toFile());
        }
        if (cfg.mtls()) {
            builder.clientAuth(ClientAuth.REQUIRE);
        }
        return GrpcSslContexts.configure(builder).build();
    }

    public static SslContext forClient(TlsConfig cfg) throws SSLException {
        if (cfg == null) return null;
        var builder = SslContextBuilder.forClient();
        if (cfg.trustCerts() != null) {
            builder.trustManager(cfg.trustCerts().toFile());
        }
        if (cfg.certChain() != null && cfg.privateKey() != null) {
            builder.keyManager(cfg.certChain().toFile(), cfg.privateKey().toFile());
        }
        return GrpcSslContexts.configure(builder).build();
    }
}
