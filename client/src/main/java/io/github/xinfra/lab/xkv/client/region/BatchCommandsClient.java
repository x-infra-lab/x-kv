package io.github.xinfra.lab.xkv.client.region;

import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.github.xinfra.lab.xkv.proto.Tikvpb;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multiplexes RPC requests over a bidi {@code BatchCommands} stream per store.
 *
 * <p>Each call to {@link #send} enqueues a single sub-request into the open
 * stream and returns a future that completes when the server responds with the
 * matching {@code request_id}. The stream is opened lazily on first use and
 * reconnected on error.
 *
 * <p>This mirrors TiKV client-java's {@code BatchCommandsClient}: one
 * long-lived bidi stream replaces N independent blocking gRPC calls, enabling
 * server-side fsync amortization across concurrent requests.
 */
public final class BatchCommandsClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BatchCommandsClient.class);

    private final StoreChannelCache storeCache;
    private final ConcurrentHashMap<Long, StoreStream> streams = new ConcurrentHashMap<>();

    public BatchCommandsClient(StoreChannelCache storeCache) {
        this.storeCache = storeCache;
    }

    /**
     * Send a single sub-request to the given store via the BatchCommands stream.
     *
     * @return a future that completes with the server's response for this request
     */
    public CompletableFuture<Tikvpb.BatchCommandsResponse.Response> send(
            long storeId, Tikvpb.BatchCommandsRequest.Request req) {
        var stream = streams.computeIfAbsent(storeId, id -> new StoreStream(id, storeCache));
        return stream.send(req);
    }

    public void closeStore(long storeId) {
        var s = streams.remove(storeId);
        if (s != null) s.close();
    }

    @Override
    public void close() {
        for (var s : streams.values()) s.close();
        streams.clear();
    }

    /**
     * Per-store bidi stream holder. Thread-safe: multiple callers can invoke
     * {@link #send} concurrently; requests are serialized via
     * {@code synchronized} on the {@code StreamObserver.onNext} call.
     */
    private static final class StoreStream {
        private final long storeId;
        private final StoreChannelCache storeCache;
        private final AtomicLong nextRequestId = new AtomicLong(1);
        private final ConcurrentHashMap<Long, CompletableFuture<Tikvpb.BatchCommandsResponse.Response>>
                pending = new ConcurrentHashMap<>();

        private volatile StreamObserver<Tikvpb.BatchCommandsRequest> requestObserver;
        private volatile boolean closed;

        StoreStream(long storeId, StoreChannelCache storeCache) {
            this.storeId = storeId;
            this.storeCache = storeCache;
        }

        CompletableFuture<Tikvpb.BatchCommandsResponse.Response> send(
                Tikvpb.BatchCommandsRequest.Request req) {
            long reqId = nextRequestId.getAndIncrement();
            var fut = new CompletableFuture<Tikvpb.BatchCommandsResponse.Response>();
            pending.put(reqId, fut);

            try {
                var obs = ensureStream();
                var batch = Tikvpb.BatchCommandsRequest.newBuilder()
                        .addRequests(req)
                        .addRequestIds(reqId)
                        .build();
                synchronized (obs) {
                    obs.onNext(batch);
                }
            } catch (Throwable t) {
                pending.remove(reqId);
                fut.completeExceptionally(t);
                resetStream();
            }
            return fut;
        }

        private synchronized StreamObserver<Tikvpb.BatchCommandsRequest> ensureStream() {
            if (requestObserver != null && !closed) return requestObserver;
            var asyncStub = storeCache.asyncStubFor(storeId);
            if (asyncStub == null) {
                throw new RuntimeException("store " + storeId + " unreachable");
            }
            requestObserver = asyncStub.batchCommands(new StreamObserver<>() {
                @Override
                public void onNext(Tikvpb.BatchCommandsResponse batch) {
                    for (int i = 0; i < batch.getResponsesCount(); i++) {
                        long id = i < batch.getRequestIdsCount() ? batch.getRequestIds(i) : -1;
                        var fut = pending.remove(id);
                        if (fut != null) {
                            fut.complete(batch.getResponses(i));
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.debug("store {} batchCommands stream error: {}", storeId, t.getMessage());
                    failAll(t);
                    resetStream();
                }

                @Override
                public void onCompleted() {
                    failAll(new RuntimeException("stream completed by server"));
                    resetStream();
                }
            });
            return requestObserver;
        }

        private void failAll(Throwable cause) {
            for (var entry : pending.entrySet()) {
                var fut = pending.remove(entry.getKey());
                if (fut != null) fut.completeExceptionally(cause);
            }
        }

        private synchronized void resetStream() {
            requestObserver = null;
        }

        void close() {
            closed = true;
            var obs = requestObserver;
            if (obs != null) {
                try { obs.onCompleted(); } catch (Throwable ignored) {}
            }
            failAll(new RuntimeException("client closed"));
            requestObserver = null;
        }
    }
}
