package io.github.xinfra.lab.xkv.pd.server;

import io.github.xinfra.lab.xkv.common.metrics.MetricsHttpServer;
import io.github.xinfra.lab.xkv.pd.config.PdScheduleConfigManager;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.LeaderBalanceScheduler;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.OperatorQueue;
import io.github.xinfra.lab.xkv.pd.state.RegionBalanceScheduler;
import io.github.xinfra.lab.xkv.pd.state.SchedulerManager;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PdHttpApiTest {

    private MetricsHttpServer httpServer;
    private SchedulerManager schedulerManager;
    private PdScheduleConfigManager configManager;
    private InMemoryPdStateMachine state;
    private HttpClient client;
    private int port;

    private LeaderBalanceScheduler leaderBalance;
    private RegionBalanceScheduler regionBalance;

    @BeforeEach
    void setUp() throws IOException {
        state = new InMemoryPdStateMachine();
        schedulerManager = new SchedulerManager();
        configManager = new PdScheduleConfigManager();

        var controller = new OperatorControllerImpl(new OperatorQueue(), 5, 600_000);
        leaderBalance = new LeaderBalanceScheduler(state, controller, 60_000);
        regionBalance = new RegionBalanceScheduler(state, controller, 60_000);
        schedulerManager.register("leader-balance", leaderBalance);
        schedulerManager.register("region-balance", regionBalance);

        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        httpServer = new MetricsHttpServer(0, registry);
        port = httpServer.port();

        var api = new PdHttpApi(schedulerManager, configManager, state);
        api.register(httpServer);

        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        leaderBalance.close();
        regionBalance.close();
        httpServer.close();
    }

    @Test
    void listSchedulers() throws Exception {
        var resp = get("/pd/api/v1/schedulers");
        assertThat(resp.statusCode()).isEqualTo(200);
        String body = resp.body();
        assertThat(body).contains("\"leader-balance\"");
        assertThat(body).contains("\"region-balance\"");
        assertThat(body).contains("\"paused\":false");
    }

    @Test
    void pauseScheduler() throws Exception {
        var resp = post("/pd/api/v1/schedulers/leader-balance/pause", "");
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("\"paused\"");
        assertThat(leaderBalance.isPaused()).isTrue();
    }

    @Test
    void resumeScheduler() throws Exception {
        leaderBalance.pause();
        var resp = post("/pd/api/v1/schedulers/leader-balance/resume", "");
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("\"running\"");
        assertThat(leaderBalance.isPaused()).isFalse();
    }

    @Test
    void pauseNonexistent() throws Exception {
        var resp = post("/pd/api/v1/schedulers/nonexistent/pause", "");
        assertThat(resp.statusCode()).isEqualTo(404);
    }

    @Test
    void getConfig() throws Exception {
        var resp = get("/pd/api/v1/config/schedule");
        assertThat(resp.statusCode()).isEqualTo(200);
        String body = resp.body();
        assertThat(body).contains("schedule.max-operators-per-store");
        assertThat(body).contains("schedule.region-split-bytes");
    }

    @Test
    void updateConfig() throws Exception {
        var resp = post("/pd/api/v1/config/schedule",
                "{\"schedule.max-operators-per-store\": \"10\"}");
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(configManager.get("schedule.max-operators-per-store")).isEqualTo("10");
    }

    @Test
    void updateConfigUnknownKey() throws Exception {
        var resp = post("/pd/api/v1/config/schedule",
                "{\"nonexistent.key\": \"value\"}");
        assertThat(resp.statusCode()).isEqualTo(400);
        assertThat(resp.body()).contains("unknown config key");
    }

    @Test
    void status() throws Exception {
        var resp = get("/pd/api/v1/status");
        assertThat(resp.statusCode()).isEqualTo(200);
        String body = resp.body();
        assertThat(body).contains("\"store_count\":");
        assertThat(body).contains("\"region_count\":");
        assertThat(body).contains("\"scheduler_count\":2");
    }

    @Test
    void parseJsonObjectRoundTrip() {
        var parsed = PdHttpApi.parseJsonObject("{\"a\": \"1\", \"b\": \"2\"}");
        assertThat(parsed).isEqualTo(Map.of("a", "1", "b", "2"));
    }

    @Test
    void parseJsonObjectEmpty() {
        assertThat(PdHttpApi.parseJsonObject("{}")).isEmpty();
        assertThat(PdHttpApi.parseJsonObject(null)).isNull();
        assertThat(PdHttpApi.parseJsonObject("")).isNull();
        assertThat(PdHttpApi.parseJsonObject("not json")).isNull();
    }

    private HttpResponse<String> get(String path) throws Exception {
        var req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        var req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
