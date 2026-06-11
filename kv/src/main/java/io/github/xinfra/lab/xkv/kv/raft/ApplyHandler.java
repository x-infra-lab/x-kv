package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;

/**
 * Pluggable handler the apply loop calls for each committed Raft entry.
 *
 * <p>The handler MUST stage all CF mutations the entry implies into the
 * supplied {@link StorageEngine.WriteBatch}. The apply loop will append
 * the new applied-index to the same batch and flush it with one fsync
 * (Inv-1). Returning a non-success result causes the proposal future to
 * complete with that failure but does NOT prevent the entry from being
 * marked applied — the entry already happened on this node, and replaying
 * it on another node would be incorrect.
 */
public interface ApplyHandler {

    /**
     * Apply one normal entry. The handler reads the kind tag and routes to
     * a per-kind sub-handler.
     *
     * @param decoded   Pre-decoded ProposalCodec output
     * @param batch     The shared write batch (CF mutations go here)
     */
    Result apply(ProposalCodec.Decoded decoded, StorageEngine.WriteBatch batch);

    record Result(boolean success, byte[] response, String errorMessage) {
        public static Result ok(byte[] resp) { return new Result(true, resp, null); }
        public static Result ok() { return new Result(true, null, null); }
        public static Result err(String msg) { return new Result(false, null, msg); }
    }
}
