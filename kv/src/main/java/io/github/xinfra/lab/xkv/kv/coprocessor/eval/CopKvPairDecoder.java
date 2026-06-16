package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class CopKvPairDecoder {
    private CopKvPairDecoder() {}

    public record KvPair(byte[] key, byte[] value) {}

    public static List<KvPair> decode(byte[] data) {
        if (data == null || data.length < 4) return List.of();
        ByteBuffer bb = ByteBuffer.wrap(data);
        int count = bb.getInt();
        List<KvPair> pairs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int kLen = bb.getInt();
            byte[] k = new byte[kLen];
            bb.get(k);
            int vLen = bb.getInt();
            byte[] v = new byte[vLen];
            bb.get(v);
            pairs.add(new KvPair(k, v));
        }
        return pairs;
    }
}
