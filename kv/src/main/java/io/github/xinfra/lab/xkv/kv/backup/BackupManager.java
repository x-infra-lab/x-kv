package io.github.xinfra.lab.xkv.kv.backup;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public final class BackupManager {
    private static final Logger log = LoggerFactory.getLogger(BackupManager.class);

    private static final String[] DATA_CFS = {"default", "lock", "write"};

    private final StorageEngine engine;
    private final Path tmpDir;

    public BackupManager(StorageEngine engine, Path tmpDir) {
        this.engine = engine;
        this.tmpDir = tmpDir;
    }

    public void backup(byte[] startKey, byte[] endKey,
                       Consumer<KvServerpb.BackupResponse> sink) {
        ensureTmpDir();
        long t0 = System.currentTimeMillis();

        try (var snap = engine.newSnapshot()) {
            var files = new ArrayList<KvServerpb.BackupFile>();
            for (String cfName : DATA_CFS) {
                var cf = cfFor(cfName);
                var file = backupCf(cf, cfName, snap, startKey, endKey);
                if (file != null) {
                    files.add(file);
                }
            }

            var resp = KvServerpb.BackupResponse.newBuilder()
                    .setStartKey(ByteString.copyFrom(startKey == null ? new byte[0] : startKey))
                    .setEndKey(endKey == null ? ByteString.EMPTY : ByteString.copyFrom(endKey))
                    .addAllFiles(files)
                    .build();
            sink.accept(resp);

            long elapsed = System.currentTimeMillis() - t0;
            log.info("backup completed: {} files, {}ms", files.size(), elapsed);
        }
    }

    private KvServerpb.BackupFile backupCf(StorageEngine.Cf cf, String cfName,
                                            StorageEngine.Snapshot snap,
                                            byte[] startKey, byte[] endKey) {
        byte[] lower = (startKey == null || startKey.length == 0)
                ? new byte[]{0}
                : MvccKey.lockKey(startKey);
        byte[] upper = (endKey != null && endKey.length > 0)
                ? MvccKey.lockKey(endKey)
                : null;

        Path sstPath = tmpDir.resolve("backup-" + cfName + "-" + Thread.currentThread().getId() + ".sst");
        long kvCount = 0;
        long totalBytes = 0;

        try (var envOpts = new EnvOptions();
             var opts = new Options();
             var writer = new SstFileWriter(envOpts, opts)) {

            writer.open(sstPath.toString());

            try (var ro = engine.newReadOptions().snapshot(snap);
                 var it = engine.newIterator(cf, ro)) {
                for (it.seek(lower); it.isValid(); it.next()) {
                    byte[] k = it.key();
                    if (upper != null && Arrays.compareUnsigned(k, upper) >= 0) break;
                    byte[] v = it.value();
                    writer.put(k, v);
                    kvCount++;
                    totalBytes += k.length + v.length;
                }
            }

            if (kvCount == 0) {
                writer.close();
                Files.deleteIfExists(sstPath);
                return null;
            }

            writer.finish();

        } catch (Exception e) {
            try { Files.deleteIfExists(sstPath); } catch (IOException ignored) {}
            throw new RuntimeException("backup CF " + cfName + " failed", e);
        }

        try {
            byte[] sstBytes = Files.readAllBytes(sstPath);
            byte[] sha256 = sha256(sstBytes);
            Files.deleteIfExists(sstPath);

            return KvServerpb.BackupFile.newBuilder()
                    .setName(cfName + ".sst")
                    .setCf(cfName)
                    .setStartKey(ByteString.copyFrom(startKey == null ? new byte[0] : startKey))
                    .setEndKey(endKey == null ? ByteString.EMPTY : ByteString.copyFrom(endKey))
                    .setTotalKvs(kvCount)
                    .setTotalBytes(totalBytes)
                    .setSize(sstBytes.length)
                    .setSha256(ByteString.copyFrom(sha256))
                    .setData(ByteString.copyFrom(sstBytes))
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("reading backup SST failed", e);
        }
    }

    private void ensureTmpDir() {
        try {
            Files.createDirectories(tmpDir);
        } catch (IOException e) {
            throw new RuntimeException("failed to create backup tmp dir: " + tmpDir, e);
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

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
