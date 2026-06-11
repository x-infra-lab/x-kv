package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;

/**
 * Apply handler for raw-KV operations (RAW_PUT / RAW_DELETE / RAW_DELETE_RANGE / RAW_CAS).
 *
 * <p>v1 mixed raw KV and MVCC into a single {@code default} CF, so a raw
 * scan returned MVCC-encoded keys and a raw get against an MVCC-only key
 * found nothing. The v2 contract is: <strong>raw KV writes go to the
 * {@code default} CF using the user-key directly</strong>. MVCC writes go
 * to {@code default} too, but with the trailing 8-byte commit_ts suffix
 * that distinguishes them. A bare key without suffix is a raw entry; the
 * write CF separates them at scan time.
 *
 * <p>For Phase 1 we have no MVCC; raw keys are the only inhabitants of
 * the {@code default} CF. The reservation comment is here so Phase 2's
 * MVCC layer can keep the contract.
 *
 * <p>RAW_CAS reads the current value (under the apply path's serialization
 * — same writer-lock that the apply loop holds, so no concurrent mutation
 * can race) and conditionally writes the new value. The result encodes
 * back into a {@link Kvrpcpb.RawCASResponse} so the caller sees whether
 * the swap succeeded plus the previous value for diagnostics.
 */
public final class RawKvApplyHandler implements ApplyHandler {

    private final StorageEngine engine;

    public RawKvApplyHandler() { this(null); }
    public RawKvApplyHandler(StorageEngine engine) { this.engine = engine; }

    @Override
    public Result apply(ProposalCodec.Decoded decoded, StorageEngine.WriteBatch batch) {
        return switch (decoded.kind()) {
            case RAW_PUT -> applyPut(decoded.payload(), batch);
            case RAW_DELETE -> applyDelete(decoded.payload(), batch);
            case RAW_DELETE_RANGE -> applyDeleteRange(decoded.payload(), batch);
            case RAW_CAS -> applyCas(decoded.payload(), batch);
            default -> Result.err("unsupported kind: " + decoded.kind());
        };
    }

    private Result applyPut(byte[] payload, StorageEngine.WriteBatch batch) {
        var op = RawKvCodec.decodePut(payload);
        batch.put(StorageEngine.Cf.DEFAULT, op.key(), op.value());
        return Result.ok();
    }

    private Result applyDelete(byte[] payload, StorageEngine.WriteBatch batch) {
        var op = RawKvCodec.decodeDelete(payload);
        batch.delete(StorageEngine.Cf.DEFAULT, op.key());
        return Result.ok();
    }

    private Result applyDeleteRange(byte[] payload, StorageEngine.WriteBatch batch) {
        var op = RawKvCodec.decodeDeleteRange(payload);
        batch.deleteRange(StorageEngine.Cf.DEFAULT, op.start(), op.end());
        return Result.ok();
    }

    private Result applyCas(byte[] payload, StorageEngine.WriteBatch batch) {
        if (engine == null) return Result.err("RAW_CAS apply requires StorageEngine");
        Kvrpcpb.RawCASRequest req;
        try { req = Kvrpcpb.RawCASRequest.parseFrom(payload); }
        catch (Exception e) { return Result.err("RAW_CAS decode: " + e.getMessage()); }

        byte[] key = req.getKey().toByteArray();
        byte[] current = engine.get(StorageEngine.Cf.DEFAULT, key);
        boolean expectedAbsent = req.getPreviousNotExist();
        byte[] expected = req.getPreviousValue().toByteArray();

        boolean matches = expectedAbsent
                ? (current == null)
                : (current != null && java.util.Arrays.equals(current, expected));

        var resp = Kvrpcpb.RawCASResponse.newBuilder().setSucceed(matches);
        if (current == null) {
            resp.setPreviousNotExist(true);
        } else {
            resp.setPreviousValue(com.google.protobuf.ByteString.copyFrom(current));
        }

        if (matches) {
            batch.put(StorageEngine.Cf.DEFAULT, key, req.getValue().toByteArray());
        }
        return Result.ok(resp.build().toByteArray());
    }
}
