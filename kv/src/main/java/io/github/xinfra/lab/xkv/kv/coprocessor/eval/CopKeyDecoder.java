package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

public final class CopKeyDecoder {
    private CopKeyDecoder() {}

    public static long decodeRowHandle(byte[] rowKey) {
        return CopCodecUtil.decodeInt64(rowKey, 11);
    }
}
