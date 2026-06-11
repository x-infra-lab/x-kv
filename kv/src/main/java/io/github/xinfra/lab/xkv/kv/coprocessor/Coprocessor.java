package io.github.xinfra.lab.xkv.kv.coprocessor;

import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;

import java.util.function.Consumer;

/**
 * Pushed-down compute over a single region's key range.
 *
 * <p>One implementation per request type (e.g. SQL TableScan, IndexScan,
 * Aggregation). The dispatcher routes by {@code request.tp}.
 *
 * <p>Streaming variant ({@link #handleStream}) is the load-bearing API —
 * one-shot {@link #handle} should ONLY be used for small results that fit
 * comfortably in one response message. The v1 spec used {@code repeated KvPair}
 * exclusively, which forced full materialization for any scan.
 */
public interface Coprocessor {

    int requestType();

    Response handle(Request req);

    /**
     * Streaming handle. Each chunk should be size-bounded (≤ ~4 MiB) so the
     * client's BatchCommands stream stays even.
     */
    void handleStream(Request req, Consumer<StreamResponse> sink);
}
