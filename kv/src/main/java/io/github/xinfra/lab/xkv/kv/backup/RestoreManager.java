package io.github.xinfra.lab.xkv.kv.backup;

import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RestoreManager {
    private static final Logger log = LoggerFactory.getLogger(RestoreManager.class);

    private final StorageEngine engine;
    private final Path tmpDir;

    public RestoreManager(StorageEngine engine, Path tmpDir) {
        this.engine = engine;
        this.tmpDir = tmpDir;
    }

    public void restore(List<KvServerpb.BackupFile> ssts) {
        if (ssts == null || ssts.isEmpty()) return;
        ensureTmpDir();

        Map<StorageEngine.Cf, List<Path>> byCf = new HashMap<>();
        List<Path> tempFiles = new ArrayList<>();

        try {
            for (int i = 0; i < ssts.size(); i++) {
                var sst = ssts.get(i);
                var cf = cfFor(sst.getCf());
                Path tempFile = tmpDir.resolve("restore-" + sst.getCf() + "-" + i + "-"
                        + Thread.currentThread().getId() + ".sst");
                Files.write(tempFile, sst.getData().toByteArray());
                tempFiles.add(tempFile);
                byCf.computeIfAbsent(cf, k -> new ArrayList<>()).add(tempFile);
            }

            for (var entry : byCf.entrySet()) {
                engine.ingestSst(entry.getKey(), entry.getValue());
            }

            log.info("restore completed: {} SST files ingested across {} CFs",
                    ssts.size(), byCf.size());
        } catch (IOException e) {
            throw new RuntimeException("restore failed", e);
        } finally {
            for (Path f : tempFiles) {
                try { Files.deleteIfExists(f); } catch (IOException ignored) {}
            }
        }
    }

    private void ensureTmpDir() {
        try {
            Files.createDirectories(tmpDir);
        } catch (IOException e) {
            throw new RuntimeException("failed to create restore tmp dir: " + tmpDir, e);
        }
    }

    private static StorageEngine.Cf cfFor(String cfName) {
        return switch (cfName) {
            case "default" -> StorageEngine.Cf.DEFAULT;
            case "lock" -> StorageEngine.Cf.LOCK;
            case "write" -> StorageEngine.Cf.WRITE;
            default -> throw new IllegalArgumentException("unknown CF: " + cfName);
        };
    }
}
