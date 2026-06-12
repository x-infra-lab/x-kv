package io.github.xinfra.lab.xkv.common.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads YAML config into a flat key-value map with dot-separated keys.
 *
 * <p>Supports three layers (highest priority first):
 * <ol>
 *   <li>CLI arguments ({@code --key value})</li>
 *   <li>Environment variables ({@code PREFIX_KEY_NAME})</li>
 *   <li>YAML file</li>
 * </ol>
 */
public final class YamlConfigLoader {

    private YamlConfigLoader() {}

    public static Map<String, String> load(Path yamlFile) throws IOException {
        var map = new LinkedHashMap<String, String>();
        if (yamlFile != null && Files.exists(yamlFile)) {
            try (Reader r = Files.newBufferedReader(yamlFile)) {
                var yaml = new Yaml();
                Map<String, Object> raw = yaml.load(r);
                if (raw != null) {
                    flatten("", raw, map);
                }
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Map<String, Object> map,
                                 Map<String, String> out) {
        for (var entry : map.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object val = entry.getValue();
            if (val instanceof Map) {
                flatten(key, (Map<String, Object>) val, out);
            } else if (val != null) {
                out.put(key, val.toString());
            }
        }
    }

    /**
     * Merge environment variables with the given prefix into the map.
     * Convention: {@code X_KV_STORE_ID} → key {@code store-id}.
     */
    public static void mergeEnvVars(String prefix, Map<String, String> map) {
        mergeEnvVars(prefix, map, System.getenv());
    }

    static void mergeEnvVars(String prefix, Map<String, String> map,
                              Map<String, String> env) {
        String pfx = prefix.endsWith("_") ? prefix : prefix + "_";
        for (var entry : env.entrySet()) {
            if (!entry.getKey().startsWith(pfx)) continue;
            String suffix = entry.getKey().substring(pfx.length());
            String key = suffix.toLowerCase().replace('_', '-');
            map.put(key, entry.getValue());
        }
    }

    /**
     * Parse CLI arguments ({@code --key value}) and overlay onto the map.
     */
    public static void mergeCliArgs(String[] args, Map<String, String> map) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                map.put(key, args[i + 1]);
                i++;
            }
        }
    }

    /**
     * Full config loading pipeline: YAML → env vars → CLI args.
     *
     * @param args     CLI arguments
     * @param envPrefix environment variable prefix (e.g., "X_KV")
     * @return merged config map
     */
    public static Map<String, String> loadAll(String[] args, String envPrefix) throws IOException {
        String configPath = findArg(args, "--config", findArg(args, "-c", null));
        var map = configPath != null ? load(Path.of(configPath)) : new LinkedHashMap<String, String>();
        mergeEnvVars(envPrefix, map);
        mergeCliArgs(args, map);
        return map;
    }

    public static String findArg(String[] args, String flag, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) return args[i + 1];
        }
        return def;
    }

    public static String getString(Map<String, String> m, String key, String def) {
        return m.getOrDefault(key, def);
    }

    public static long getLong(Map<String, String> m, String key, long def) {
        String v = m.get(key);
        return v != null ? Long.parseLong(v) : def;
    }

    public static int getInt(Map<String, String> m, String key, int def) {
        String v = m.get(key);
        return v != null ? Integer.parseInt(v) : def;
    }

    public static boolean getBool(Map<String, String> m, String key, boolean def) {
        String v = m.get(key);
        return v != null ? Boolean.parseBoolean(v) : def;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getStringList(Map<String, String> m, String key, List<String> def) {
        String v = m.get(key);
        if (v == null) return def;
        v = v.trim();
        if (v.startsWith("[") && v.endsWith("]")) {
            v = v.substring(1, v.length() - 1);
        }
        if (v.isEmpty()) return def;
        var result = new java.util.ArrayList<String>();
        for (String s : v.split(",")) {
            s = s.trim();
            if (s.startsWith("\"") && s.endsWith("\"")) s = s.substring(1, s.length() - 1);
            if (s.startsWith("'") && s.endsWith("'")) s = s.substring(1, s.length() - 1);
            if (!s.isEmpty()) result.add(s);
        }
        return result;
    }
}
