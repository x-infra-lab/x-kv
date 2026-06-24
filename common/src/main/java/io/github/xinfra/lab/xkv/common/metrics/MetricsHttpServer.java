package io.github.xinfra.lab.xkv.common.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public final class MetricsHttpServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MetricsHttpServer.class);

    private static final byte[] OK = "OK".getBytes(StandardCharsets.UTF_8);
    private static final byte[] UNAVAILABLE = "Service Unavailable".getBytes(StandardCharsets.UTF_8);

    private static final byte[] UNAUTHORIZED = "Unauthorized".getBytes(StandardCharsets.UTF_8);

    private final HttpServer httpServer;

    public MetricsHttpServer(int port, PrometheusMeterRegistry registry) throws IOException {
        this(port, registry, () -> true, null);
    }

    public MetricsHttpServer(int port, PrometheusMeterRegistry registry,
                             Supplier<Boolean> readinessChecker) throws IOException {
        this(port, registry, readinessChecker, null);
    }

    public MetricsHttpServer(int port, PrometheusMeterRegistry registry,
                             Supplier<Boolean> readinessChecker,
                             String metricsAuthToken) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/metrics", exchange -> {
            if (metricsAuthToken != null) {
                String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
                if (authHeader == null || !authHeader.equals("Bearer " + metricsAuthToken)) {
                    exchange.sendResponseHeaders(401, UNAUTHORIZED.length);
                    try (var os = exchange.getResponseBody()) { os.write(UNAUTHORIZED); }
                    return;
                }
            }
            String body = registry.scrape();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type",
                    "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        httpServer.createContext("/healthz", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, OK.length);
            try (var os = exchange.getResponseBody()) {
                os.write(OK);
            }
        });
        httpServer.createContext("/readyz", exchange -> {
            boolean ready;
            try {
                ready = readinessChecker.get();
            } catch (Throwable t) {
                ready = false;
            }
            int code = ready ? 200 : 503;
            byte[] body = ready ? OK : UNAVAILABLE;
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(code, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        httpServer.setExecutor(null);
        httpServer.start();
        log.info("Metrics HTTP server listening on port {}", port);
    }

    public void addContext(String path, com.sun.net.httpserver.HttpHandler handler) {
        httpServer.createContext(path, handler);
    }

    public int port() {
        return httpServer.getAddress().getPort();
    }

    @Override
    public void close() {
        httpServer.stop(0);
    }
}
