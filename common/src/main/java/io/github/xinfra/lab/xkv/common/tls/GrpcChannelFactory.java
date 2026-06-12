package io.github.xinfra.lab.xkv.common.tls;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.net.InetSocketAddress;
import java.util.List;

public final class GrpcChannelFactory {

    public static final int MAX_INBOUND_MESSAGE_SIZE = 16 * 1024 * 1024;

    private GrpcChannelFactory() {}

    public static ManagedChannel build(String address, TlsConfig tls,
                                       List<ClientInterceptor> interceptors) {
        var hp = parseHostPort(address);
        var builder = NettyChannelBuilder.forAddress(hp.host(), hp.port())
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE);
        if (tls != null) {
            try {
                var sslCtx = SslContextFactory.forClient(tls);
                if (sslCtx != null) builder.sslContext(sslCtx);
                else builder.usePlaintext();
            } catch (Exception e) {
                throw new RuntimeException("failed to build client TLS context", e);
            }
        } else {
            builder.usePlaintext();
        }
        if (interceptors != null) {
            for (var i : interceptors) builder.intercept(i);
        }
        return builder.build();
    }

    public static ManagedChannel build(String address, TlsConfig tls) {
        return build(address, tls, null);
    }

    public static NettyServerBuilder serverBuilder(InetSocketAddress addr, TlsConfig tls) {
        var builder = NettyServerBuilder.forAddress(addr)
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE);
        if (tls != null) {
            try {
                var sslCtx = SslContextFactory.forServer(tls);
                if (sslCtx != null) builder.sslContext(sslCtx);
            } catch (Exception e) {
                throw new RuntimeException("failed to build server TLS context", e);
            }
        }
        return builder;
    }

    public static HostPort parseHostPort(String s) {
        var i = s.lastIndexOf(':');
        return new HostPort(s.substring(0, i), Integer.parseInt(s.substring(i + 1)));
    }

    public record HostPort(String host, int port) {}
}
