package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.common.tls.TlsConfig;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
final class TlsE2ETest {

    @TempDir Path certDir;

    private Path serverCert;
    private Path serverKey;
    private Path clientCert;
    private Path clientKey;

    private Server server;
    private final List<ManagedChannel> channels = new ArrayList<>();

    @BeforeEach
    void generateCerts() throws Exception {
        serverCert = certDir.resolve("server.crt");
        serverKey = certDir.resolve("server.key");
        clientCert = certDir.resolve("client.crt");
        clientKey = certDir.resolve("client.key");

        generateSelfSignedCert("localhost", "server", certDir);
        generateSelfSignedCert("client", "client", certDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (var ch : channels) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception e) { e.printStackTrace(); }
        }
        channels.clear();
        if (server != null) server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        ClusterHarness.releaseAllPorts();
    }

    private ManagedChannel track(ManagedChannel ch) {
        channels.add(ch);
        return ch;
    }

    @Test
    void tlsServerRejectsPlaintextClient() throws Exception {
        var serverTls = new TlsConfig(serverCert, serverKey, null, false);

        int port = ClusterHarness.freePort();
        ClusterHarness.releasePort(port);
        server = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress("127.0.0.1", port), serverTls)
                .addService(new TikvServiceImpl())
                .build().start();

        var ch = track(NettyChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext().build());
        var stub = TikvGrpc.newBlockingStub(ch);

        assertThatThrownBy(() -> stub.rawGet(
                Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("k1")).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void tlsClientConnectsSuccessfully() throws Exception {
        var serverTls = new TlsConfig(serverCert, serverKey, null, false);

        int port = ClusterHarness.freePort();
        ClusterHarness.releasePort(port);
        server = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress("127.0.0.1", port), serverTls)
                .addService(new TikvServiceImpl())
                .build().start();

        var tlsConfig = TlsConfig.clientOnly(serverCert);
        var ch = track(GrpcChannelFactory.build("localhost:" + port, tlsConfig));
        var stub = TikvGrpc.newBlockingStub(ch);

        // Service has no backing engine so the RPC may fail internally, but
        // the TLS handshake must succeed — no UNAVAILABLE.
        try {
            stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("k1")).build());
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.UNAVAILABLE);
        }
    }

    @Test
    void mtlsRejectsClientWithoutCert() throws Exception {
        var serverTls = TlsConfig.of(serverCert, serverKey, clientCert);

        int port = ClusterHarness.freePort();
        ClusterHarness.releasePort(port);
        server = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress("127.0.0.1", port), serverTls)
                .addService(new TikvServiceImpl())
                .build().start();

        var noClientCertTls = TlsConfig.clientOnly(serverCert);
        var ch = track(GrpcChannelFactory.build("localhost:" + port, noClientCertTls));
        var stub = TikvGrpc.newBlockingStub(ch);

        assertThatThrownBy(() -> stub.rawGet(
                Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("k1")).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void mtlsAcceptsClientWithValidCert() throws Exception {
        var serverTls = TlsConfig.of(serverCert, serverKey, clientCert);

        int port = ClusterHarness.freePort();
        ClusterHarness.releasePort(port);
        server = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress("127.0.0.1", port), serverTls)
                .addService(new TikvServiceImpl())
                .build().start();

        var tlsConfig = TlsConfig.of(clientCert, clientKey, serverCert);
        var ch = track(GrpcChannelFactory.build("localhost:" + port, tlsConfig));
        var stub = TikvGrpc.newBlockingStub(ch);

        try {
            stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("k1")).build());
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.UNAVAILABLE);
        }
    }

    private void generateSelfSignedCert(String cn, String alias, Path dir) throws Exception {
        Path p12 = dir.resolve(alias + ".p12");
        Path cert = dir.resolve(alias + ".crt");
        Path key = dir.resolve(alias + ".key");

        run(dir, "keytool", "-genkeypair",
                "-keyalg", "RSA", "-keysize", "2048",
                "-alias", alias,
                "-keystore", p12.toString(),
                "-storetype", "PKCS12",
                "-storepass", "changeit",
                "-keypass", "changeit",
                "-dname", "CN=" + cn,
                "-validity", "1",
                "-ext", "SAN=dns:" + cn);

        run(dir, "keytool", "-exportcert",
                "-alias", alias,
                "-keystore", p12.toString(),
                "-storepass", "changeit",
                "-rfc",
                "-file", cert.toString());

        run(dir, "openssl", "pkcs12",
                "-in", p12.toString(),
                "-out", key.toString(),
                "-nodes", "-nocerts",
                "-passin", "pass:changeit");
    }

    private static void run(Path workDir, String... cmd) throws Exception {
        var pb = new ProcessBuilder(cmd).directory(workDir.toFile())
                .redirectErrorStream(true);
        var proc = pb.start();
        var out = new String(proc.getInputStream().readAllBytes());
        int rc = proc.waitFor();
        if (rc != 0) {
            throw new RuntimeException("command failed (rc=" + rc + "): "
                    + String.join(" ", cmd) + "\n" + out);
        }
    }
}
