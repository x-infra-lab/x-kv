package io.github.xinfra.lab.xkv.pd.server;

import com.sun.net.httpserver.HttpExchange;
import io.github.xinfra.lab.xkv.common.metrics.MetricsHttpServer;
import io.github.xinfra.lab.xkv.pd.config.PdScheduleConfigManager;
import io.github.xinfra.lab.xkv.pd.state.PdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.SchedulerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class PdHttpApi {
    private static final Logger log = LoggerFactory.getLogger(PdHttpApi.class);

    private static final byte[] NOT_FOUND = "{\"error\":\"not found\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BAD_REQUEST = "{\"error\":\"bad request\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] METHOD_NOT_ALLOWED = "{\"error\":\"method not allowed\"}".getBytes(StandardCharsets.UTF_8);

    private final SchedulerManager schedulerManager;
    private final PdScheduleConfigManager configManager;
    private final PdStateMachine state;

    public PdHttpApi(SchedulerManager schedulerManager,
                     PdScheduleConfigManager configManager,
                     PdStateMachine state) {
        this.schedulerManager = schedulerManager;
        this.configManager = configManager;
        this.state = state;
    }

    public void register(MetricsHttpServer httpServer) {
        httpServer.addContext("/pd/api/v1/schedulers", this::handleSchedulers);
        httpServer.addContext("/pd/api/v1/config/schedule", this::handleConfig);
        httpServer.addContext("/pd/api/v1/status", this::handleStatus);
        httpServer.addContext("/pd/api/v1/keyspaces", this::handleKeyspaces);
        httpServer.addContext("/pd/api/v1/resource_groups", this::handleResourceGroups);
    }

    private void handleSchedulers(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        if ("GET".equals(method) && path.equals("/pd/api/v1/schedulers")) {
            var list = schedulerManager.list();
            var sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                var s = list.get(i);
                sb.append("{\"name\":\"").append(escapeJson(s.name()))
                  .append("\",\"running\":").append(s.running())
                  .append(",\"paused\":").append(s.paused())
                  .append("}");
            }
            sb.append("]");
            sendJson(exchange, 200, sb.toString());
            return;
        }

        if ("POST".equals(method)) {
            if (path.endsWith("/pause")) {
                String name = extractSchedulerName(path, "/pause");
                if (name != null && schedulerManager.pause(name)) {
                    sendJson(exchange, 200, "{\"status\":\"paused\",\"name\":\"" + escapeJson(name) + "\"}");
                } else {
                    sendJson(exchange, 404, new String(NOT_FOUND, StandardCharsets.UTF_8));
                }
                return;
            }
            if (path.endsWith("/resume")) {
                String name = extractSchedulerName(path, "/resume");
                if (name != null && schedulerManager.resume(name)) {
                    sendJson(exchange, 200, "{\"status\":\"running\",\"name\":\"" + escapeJson(name) + "\"}");
                } else {
                    sendJson(exchange, 404, new String(NOT_FOUND, StandardCharsets.UTF_8));
                }
                return;
            }
        }

        sendJson(exchange, 405, new String(METHOD_NOT_ALLOWED, StandardCharsets.UTF_8));
    }

    private void handleConfig(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();

        if ("GET".equals(method)) {
            var all = configManager.getAll();
            var sb = new StringBuilder("{");
            boolean first = true;
            for (var entry : all.entrySet()) {
                if (!first) sb.append(",");
                first = false;
                sb.append("\"").append(escapeJson(entry.getKey())).append("\":\"")
                  .append(escapeJson(entry.getValue())).append("\"");
            }
            sb.append("}");
            sendJson(exchange, 200, sb.toString());
            return;
        }

        if ("POST".equals(method)) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            Map<String, String> updates = parseJsonObject(body);
            if (updates == null) {
                sendJson(exchange, 400, new String(BAD_REQUEST, StandardCharsets.UTF_8));
                return;
            }
            var errors = new StringBuilder();
            for (var entry : updates.entrySet()) {
                String err = configManager.set(entry.getKey(), entry.getValue());
                if (err != null) {
                    if (errors.length() > 0) errors.append("; ");
                    errors.append(err);
                }
            }
            if (errors.length() > 0) {
                sendJson(exchange, 400, "{\"error\":\"" + escapeJson(errors.toString()) + "\"}");
            } else {
                sendJson(exchange, 200, "{\"status\":\"ok\"}");
            }
            return;
        }

        sendJson(exchange, 405, new String(METHOD_NOT_ALLOWED, StandardCharsets.UTF_8));
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, new String(METHOD_NOT_ALLOWED, StandardCharsets.UTF_8));
            return;
        }

        int storeCount = 0;
        for (var ignored : state.allStores()) storeCount++;
        int regionCount = 0;
        for (var ignored : state.allRegions()) regionCount++;
        int schedulerCount = schedulerManager.size();

        var sb = new StringBuilder("{");
        sb.append("\"bootstrapped\":").append(state.isBootstrapped());
        sb.append(",\"store_count\":").append(storeCount);
        sb.append(",\"region_count\":").append(regionCount);
        sb.append(",\"scheduler_count\":").append(schedulerCount);
        var cluster = state.cluster();
        if (cluster != null) {
            sb.append(",\"cluster_id\":").append(cluster.getId());
            sb.append(",\"max_peer_count\":").append(cluster.getMaxPeerCount());
        }
        sb.append("}");

        sendJson(exchange, 200, sb.toString());
    }

    private void handleKeyspaces(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        var ksm = state.keyspaceManager();
        if (ksm == null) {
            sendJson(exchange, 501, "{\"error\":\"keyspace not enabled\"}");
            return;
        }

        if ("GET".equals(method) && path.equals("/pd/api/v1/keyspaces")) {
            var list = ksm.listAll();
            var sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                appendKeyspaceMeta(sb, list.get(i));
            }
            sb.append("]");
            sendJson(exchange, 200, sb.toString());
            return;
        }

        if ("GET".equals(method) && path.startsWith("/pd/api/v1/keyspaces/")) {
            String name = path.substring("/pd/api/v1/keyspaces/".length());
            if (name.endsWith("/state")) {
                sendJson(exchange, 405, new String(METHOD_NOT_ALLOWED, StandardCharsets.UTF_8));
                return;
            }
            var meta = ksm.loadByName(name);
            if (meta == null) {
                sendJson(exchange, 404, new String(NOT_FOUND, StandardCharsets.UTF_8));
            } else {
                var sb = new StringBuilder();
                appendKeyspaceMeta(sb, meta);
                sendJson(exchange, 200, sb.toString());
            }
            return;
        }

        if ("POST".equals(method) && path.equals("/pd/api/v1/keyspaces")) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            Map<String, String> fields = parseJsonObject(body);
            if (fields == null || !fields.containsKey("name")) {
                sendJson(exchange, 400, new String(BAD_REQUEST, StandardCharsets.UTF_8));
                return;
            }
            var meta = ksm.createKeyspace(fields.get("name"), null);
            if (meta == null) {
                sendJson(exchange, 409, "{\"error\":\"keyspace already exists\"}");
            } else {
                var sb = new StringBuilder();
                appendKeyspaceMeta(sb, meta);
                sendJson(exchange, 200, sb.toString());
            }
            return;
        }

        if ("PUT".equals(method) && path.endsWith("/state")) {
            String prefix = "/pd/api/v1/keyspaces/";
            String suffix = "/state";
            if (!path.startsWith(prefix)) {
                sendJson(exchange, 404, new String(NOT_FOUND, StandardCharsets.UTF_8));
                return;
            }
            String name = path.substring(prefix.length(), path.length() - suffix.length());
            var meta = ksm.loadByName(name);
            if (meta == null) {
                sendJson(exchange, 404, new String(NOT_FOUND, StandardCharsets.UTF_8));
                return;
            }
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            Map<String, String> fields = parseJsonObject(body);
            if (fields == null || !fields.containsKey("state")) {
                sendJson(exchange, 400, new String(BAD_REQUEST, StandardCharsets.UTF_8));
                return;
            }
            io.github.xinfra.lab.xkv.proto.Pdpb.KeyspaceState newState;
            try {
                newState = io.github.xinfra.lab.xkv.proto.Pdpb.KeyspaceState.valueOf(fields.get("state"));
            } catch (IllegalArgumentException e) {
                sendJson(exchange, 400, "{\"error\":\"invalid state\"}");
                return;
            }
            var updated = ksm.updateState(meta.getId(), newState);
            if (updated == null) {
                sendJson(exchange, 400, "{\"error\":\"invalid state transition\"}");
            } else {
                var sb = new StringBuilder();
                appendKeyspaceMeta(sb, updated);
                sendJson(exchange, 200, sb.toString());
            }
            return;
        }

        sendJson(exchange, 405, new String(METHOD_NOT_ALLOWED, StandardCharsets.UTF_8));
    }

    private void handleResourceGroups(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        var rgm = state.resourceGroupManager();
        if (rgm == null) {
            sendJson(exchange, 501, "{\"error\":\"resource group not enabled\"}");
            return;
        }

        if ("GET".equals(method) && path.equals("/pd/api/v1/resource_groups")) {
            var list = rgm.listGroups();
            var sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                appendResourceGroup(sb, list.get(i));
            }
            sb.append("]");
            sendJson(exchange, 200, sb.toString());
            return;
        }

        if ("POST".equals(method) && path.equals("/pd/api/v1/resource_groups")) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            Map<String, String> fields = parseJsonObject(body);
            if (fields == null || !fields.containsKey("name")) {
                sendJson(exchange, 400, new String(BAD_REQUEST, StandardCharsets.UTF_8));
                return;
            }
            long fillRate = 0, burstLimit = 0;
            int priority = 0;
            try {
                if (fields.containsKey("fill_rate")) fillRate = Long.parseLong(fields.get("fill_rate"));
                if (fields.containsKey("burst_limit")) burstLimit = Long.parseLong(fields.get("burst_limit"));
                if (fields.containsKey("priority")) priority = Integer.parseInt(fields.get("priority"));
            } catch (NumberFormatException e) {
                sendJson(exchange, 400, new String(BAD_REQUEST, StandardCharsets.UTF_8));
                return;
            }
            var group = io.github.xinfra.lab.xkv.proto.Pdpb.ResourceGroup.newBuilder()
                    .setName(fields.get("name"))
                    .setRuSettings(io.github.xinfra.lab.xkv.proto.Pdpb.RUSettings.newBuilder()
                            .setFillRate(fillRate).setBurstLimit(burstLimit))
                    .setPriority(priority)
                    .build();
            if (!rgm.addGroup(group)) {
                sendJson(exchange, 409, "{\"error\":\"group already exists or invalid name\"}");
            } else {
                var sb = new StringBuilder();
                appendResourceGroup(sb, group);
                sendJson(exchange, 200, sb.toString());
            }
            return;
        }

        if ("PUT".equals(method) && path.startsWith("/pd/api/v1/resource_groups/")) {
            String name = path.substring("/pd/api/v1/resource_groups/".length());
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            Map<String, String> fields = parseJsonObject(body);
            if (fields == null) {
                sendJson(exchange, 400, new String(BAD_REQUEST, StandardCharsets.UTF_8));
                return;
            }
            var existing = rgm.getGroup(name);
            if (existing == null) {
                sendJson(exchange, 404, new String(NOT_FOUND, StandardCharsets.UTF_8));
                return;
            }
            long fillRate = existing.getRuSettings().getFillRate();
            long burstLimit = existing.getRuSettings().getBurstLimit();
            int priority = existing.getPriority();
            try {
                if (fields.containsKey("fill_rate")) fillRate = Long.parseLong(fields.get("fill_rate"));
                if (fields.containsKey("burst_limit")) burstLimit = Long.parseLong(fields.get("burst_limit"));
                if (fields.containsKey("priority")) priority = Integer.parseInt(fields.get("priority"));
            } catch (NumberFormatException e) {
                sendJson(exchange, 400, new String(BAD_REQUEST, StandardCharsets.UTF_8));
                return;
            }
            var updated = existing.toBuilder()
                    .setRuSettings(io.github.xinfra.lab.xkv.proto.Pdpb.RUSettings.newBuilder()
                            .setFillRate(fillRate).setBurstLimit(burstLimit))
                    .setPriority(priority)
                    .build();
            rgm.modifyGroup(updated);
            var sb = new StringBuilder();
            appendResourceGroup(sb, updated);
            sendJson(exchange, 200, sb.toString());
            return;
        }

        if ("DELETE".equals(method) && path.startsWith("/pd/api/v1/resource_groups/")) {
            String name = path.substring("/pd/api/v1/resource_groups/".length());
            if (!rgm.deleteGroup(name)) {
                sendJson(exchange, 400, "{\"error\":\"cannot delete default group or group not found\"}");
            } else {
                sendJson(exchange, 200, "{\"status\":\"deleted\"}");
            }
            return;
        }

        sendJson(exchange, 405, new String(METHOD_NOT_ALLOWED, StandardCharsets.UTF_8));
    }

    private static void appendKeyspaceMeta(StringBuilder sb, io.github.xinfra.lab.xkv.proto.Pdpb.KeyspaceMeta m) {
        sb.append("{\"id\":").append(m.getId())
          .append(",\"name\":\"").append(escapeJson(m.getName())).append("\"")
          .append(",\"state\":\"").append(m.getState().name()).append("\"")
          .append(",\"created_at\":").append(m.getCreatedAt())
          .append("}");
    }

    private static void appendResourceGroup(StringBuilder sb, io.github.xinfra.lab.xkv.proto.Pdpb.ResourceGroup g) {
        sb.append("{\"name\":\"").append(escapeJson(g.getName())).append("\"")
          .append(",\"fill_rate\":").append(g.getRuSettings().getFillRate())
          .append(",\"burst_limit\":").append(g.getRuSettings().getBurstLimit())
          .append(",\"priority\":").append(g.getPriority())
          .append(",\"background\":").append(g.getBackground())
          .append("}");
    }

    private static void sendJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String extractSchedulerName(String path, String suffix) {
        String prefix = "/pd/api/v1/schedulers/";
        if (!path.startsWith(prefix) || !path.endsWith(suffix)) return null;
        String name = path.substring(prefix.length(), path.length() - suffix.length());
        return name.isEmpty() ? null : name;
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    static Map<String, String> parseJsonObject(String json) {
        if (json == null || json.isEmpty()) return null;
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) return null;
        json = json.substring(1, json.length() - 1).trim();
        if (json.isEmpty()) return Map.of();

        var result = new java.util.LinkedHashMap<String, String>();
        int i = 0;
        while (i < json.length()) {
            i = skipWhitespace(json, i);
            if (i >= json.length()) break;

            if (json.charAt(i) == ',') { i++; continue; }

            String key = parseString(json, i);
            if (key == null) return null;
            i = skipPastClosingQuote(json, i);
            if (i < 0) return null;

            i = skipWhitespace(json, i);
            if (i >= json.length() || json.charAt(i) != ':') return null;
            i++;
            i = skipWhitespace(json, i);
            if (i >= json.length()) return null;

            String value = parseString(json, i);
            if (value == null) return null;
            i = skipPastClosingQuote(json, i);
            if (i < 0) return null;

            result.put(key, value);
        }
        return result;
    }

    private static String parseString(String json, int pos) {
        if (pos >= json.length() || json.charAt(pos) != '"') return null;
        int end = json.indexOf('"', pos + 1);
        if (end < 0) return null;
        return json.substring(pos + 1, end);
    }

    private static int skipPastClosingQuote(String json, int pos) {
        if (pos >= json.length() || json.charAt(pos) != '"') return -1;
        int end = json.indexOf('"', pos + 1);
        if (end < 0) return -1;
        return end + 1;
    }

    private static int skipWhitespace(String json, int pos) {
        while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) pos++;
        return pos;
    }
}
