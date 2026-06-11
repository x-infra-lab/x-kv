package io.github.xinfra.lab.xkv.kv.raft;

import java.nio.ByteBuffer;

/**
 * Encoding for raw-KV proposal payloads (the inner {@code payload} of a
 * {@link ProposalCodec.Kind#RAW_PUT} / {@code RAW_DELETE} entry).
 *
 * <pre>
 *   RAW_PUT:           [4B keyLen][key][4B valLen][value]
 *   RAW_DELETE:        [4B keyLen][key]
 *   RAW_DELETE_RANGE:  [4B startLen][start][4B endLen][end]
 * </pre>
 */
public final class RawKvCodec {

    private RawKvCodec() {}

    public static byte[] encodePut(byte[] key, byte[] value) {
        var bb = ByteBuffer.allocate(4 + key.length + 4 + value.length);
        bb.putInt(key.length).put(key);
        bb.putInt(value.length).put(value);
        return bb.array();
    }

    public static byte[] encodeDelete(byte[] key) {
        var bb = ByteBuffer.allocate(4 + key.length);
        bb.putInt(key.length).put(key);
        return bb.array();
    }

    public static byte[] encodeDeleteRange(byte[] start, byte[] end) {
        var bb = ByteBuffer.allocate(4 + start.length + 4 + end.length);
        bb.putInt(start.length).put(start);
        bb.putInt(end.length).put(end);
        return bb.array();
    }

    public record PutOp(byte[] key, byte[] value) {}
    public record DeleteOp(byte[] key) {}
    public record DeleteRangeOp(byte[] start, byte[] end) {}

    public static PutOp decodePut(byte[] payload) {
        var bb = ByteBuffer.wrap(payload);
        var k = new byte[bb.getInt()]; bb.get(k);
        var v = new byte[bb.getInt()]; bb.get(v);
        return new PutOp(k, v);
    }

    public static DeleteOp decodeDelete(byte[] payload) {
        var bb = ByteBuffer.wrap(payload);
        var k = new byte[bb.getInt()]; bb.get(k);
        return new DeleteOp(k);
    }

    public static DeleteRangeOp decodeDeleteRange(byte[] payload) {
        var bb = ByteBuffer.wrap(payload);
        var s = new byte[bb.getInt()]; bb.get(s);
        var e = new byte[bb.getInt()]; bb.get(e);
        return new DeleteRangeOp(s, e);
    }
}
