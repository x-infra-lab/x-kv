package io.github.xinfra.lab.xkv.common.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class YamlConfigLoaderTest {

    @TempDir Path tempDir;

    @Test
    void loadFromYamlFile_flatKeys() throws IOException {
        var yaml = tempDir.resolve("flat.yaml");
        Files.writeString(yaml, "store-id: 42\nclient-address: \"host:1234\"\n");
        var m = YamlConfigLoader.load(yaml);
        assertThat(m).containsEntry("store-id", "42");
        assertThat(m).containsEntry("client-address", "host:1234");
    }

    @Test
    void loadFromYamlFile_nestedKeys() throws IOException {
        var yaml = tempDir.resolve("nested.yaml");
        Files.writeString(yaml, """
                engine:
                  block-cache-bytes: 256
                  write-buffer-bytes: 64
                raft:
                  election-tick-ms: 1000
                """);
        var m = YamlConfigLoader.load(yaml);
        assertThat(m).containsEntry("engine.block-cache-bytes", "256");
        assertThat(m).containsEntry("engine.write-buffer-bytes", "64");
        assertThat(m).containsEntry("raft.election-tick-ms", "1000");
    }

    @Test
    void loadFromNonexistentFile_returnsEmptyMap() throws IOException {
        var m = YamlConfigLoader.load(Path.of("/no/such/file.yaml"));
        assertThat(m).isEmpty();
    }

    @Test
    void loadFromNull_returnsEmptyMap() throws IOException {
        var m = YamlConfigLoader.load(null);
        assertThat(m).isEmpty();
    }

    @Test
    void mergeEnvVars_overridesYamlKeys() throws IOException {
        var yaml = tempDir.resolve("env.yaml");
        Files.writeString(yaml, "store-id: 1\n");
        var m = YamlConfigLoader.load(yaml);
        YamlConfigLoader.mergeEnvVars("X_KV", m, Map.of("X_KV_STORE_ID", "99"));
        assertThat(m).containsEntry("store-id", "99");
    }

    @Test
    void mergeEnvVars_convertsCaseAndUnderscoresToDashes() {
        var m = new LinkedHashMap<String, String>();
        YamlConfigLoader.mergeEnvVars("X_KV", m,
                Map.of("X_KV_CLIENT_ADDRESS", "0.0.0.0:20160"));
        assertThat(m).containsEntry("client-address", "0.0.0.0:20160");
    }

    @Test
    void mergeEnvVars_ignoresUnrelatedPrefixes() {
        var m = new LinkedHashMap<String, String>();
        YamlConfigLoader.mergeEnvVars("X_KV", m,
                Map.of("X_PD_NODE_ID", "5", "HOME", "/root"));
        assertThat(m).isEmpty();
    }

    @Test
    void mergeCliArgs_overridesAll() {
        var m = new LinkedHashMap<String, String>();
        m.put("store-id", "1");
        YamlConfigLoader.mergeCliArgs(
                new String[]{"--store-id", "7", "--data-dir", "/tmp/x"}, m);
        assertThat(m).containsEntry("store-id", "7");
        assertThat(m).containsEntry("data-dir", "/tmp/x");
    }

    @Test
    void mergeCliArgs_oddArgumentCountIgnoresTrailing() {
        var m = new LinkedHashMap<String, String>();
        YamlConfigLoader.mergeCliArgs(new String[]{"--store-id"}, m);
        assertThat(m).doesNotContainKey("store-id");
    }

    @Test
    void loadAll_fullPipeline() throws IOException {
        var yaml = tempDir.resolve("full.yaml");
        Files.writeString(yaml, "store-id: 1\nclient-address: from-yaml\n");
        var m = YamlConfigLoader.loadAll(
                new String[]{"-c", yaml.toString(), "--store-id", "99"}, "X_KV");
        assertThat(m).containsEntry("store-id", "99");
        assertThat(m).containsEntry("client-address", "from-yaml");
    }

    @Test
    void getInt_parsesAndDefaults() {
        var m = Map.of("port", "8080");
        assertThat(YamlConfigLoader.getInt(m, "port", 0)).isEqualTo(8080);
        assertThat(YamlConfigLoader.getInt(m, "missing", 42)).isEqualTo(42);
        assertThatThrownBy(() -> YamlConfigLoader.getInt(Map.of("bad", "abc"), "bad", 0))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void getLong_parsesAndDefaults() {
        var m = Map.of("size", "1099511627776");
        assertThat(YamlConfigLoader.getLong(m, "size", 0)).isEqualTo(1099511627776L);
        assertThat(YamlConfigLoader.getLong(m, "missing", -1)).isEqualTo(-1);
    }

    @Test
    void getBool_parsesAndDefaults() {
        var m = Map.of("enabled", "true", "disabled", "false");
        assertThat(YamlConfigLoader.getBool(m, "enabled", false)).isTrue();
        assertThat(YamlConfigLoader.getBool(m, "disabled", true)).isFalse();
        assertThat(YamlConfigLoader.getBool(m, "missing", true)).isTrue();
    }

    @Test
    void getStringList_parsesFormats() {
        var def = List.of("fallback");
        assertThat(YamlConfigLoader.getStringList(Map.of("k", "[a, b, c]"), "k", def))
                .containsExactly("a", "b", "c");
        assertThat(YamlConfigLoader.getStringList(Map.of("k", "[\"x\",\"y\"]"), "k", def))
                .containsExactly("x", "y");
        assertThat(YamlConfigLoader.getStringList(Map.of("k", "'p','q'"), "k", def))
                .containsExactly("p", "q");
        assertThat(YamlConfigLoader.getStringList(Map.of("k", "a,b,c"), "k", def))
                .containsExactly("a", "b", "c");
        assertThat(YamlConfigLoader.getStringList(Map.of(), "k", def))
                .containsExactly("fallback");
        assertThat(YamlConfigLoader.getStringList(Map.of("k", "[]"), "k", def))
                .containsExactly("fallback");
    }

    @Test
    void findArg_findsAndMissesFlags() {
        String[] args = {"--config", "/etc/x.yaml", "-c", "/alt.yaml", "--debug"};
        assertThat(YamlConfigLoader.findArg(args, "--config", null)).isEqualTo("/etc/x.yaml");
        assertThat(YamlConfigLoader.findArg(args, "-c", null)).isEqualTo("/alt.yaml");
        assertThat(YamlConfigLoader.findArg(args, "--missing", "def")).isEqualTo("def");
        assertThat(YamlConfigLoader.findArg(args, "--debug", null))
                .as("--debug is last arg with no value following")
                .isNull();
    }
}
