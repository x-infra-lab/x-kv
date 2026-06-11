package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.common.metrics.MetricsHttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

final class HealthCheckE2ETest {

    private PrometheusMeterRegistry registry;
    private MetricsHttpServer metricsHttp;

    @AfterEach
    void tearDown() {
        if (metricsHttp != null) metricsHttp.close();
        if (registry != null) registry.close();
    }

    @Test
    void healthzAlwaysReturns200() throws Exception {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        metricsHttp = new MetricsHttpServer(0, registry, () -> false);
        int port = metricsHttp.port();

        var client = HttpClient.newHttpClient();
        var resp = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/healthz"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).isEqualTo("OK");
    }

    @Test
    void readyzReflectsReadinessChecker() throws Exception {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        var ready = new AtomicBoolean(false);
        metricsHttp = new MetricsHttpServer(0, registry, ready::get);
        int port = metricsHttp.port();

        var client = HttpClient.newHttpClient();

        // Not ready → 503
        var resp = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/readyz"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).isEqualTo("Service Unavailable");

        // Become ready → 200
        ready.set(true);
        resp = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/readyz"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).isEqualTo("OK");
    }

    @Test
    void readyzDefaultsToReady() throws Exception {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        metricsHttp = new MetricsHttpServer(0, registry);
        int port = metricsHttp.port();

        var client = HttpClient.newHttpClient();
        var resp = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/readyz"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    @Test
    void metricsEndpointStillWorks() throws Exception {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        metricsHttp = new MetricsHttpServer(0, registry);
        int port = metricsHttp.port();

        var client = HttpClient.newHttpClient();
        var resp = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/metrics"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
    }
}
