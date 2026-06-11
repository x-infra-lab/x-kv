package io.github.xinfra.lab.xkv.kv.server;

import io.github.xinfra.lab.xkv.kv.coprocessor.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Coprocessor.BatchRequest;
import io.github.xinfra.lab.xkv.proto.Coprocessor.BatchResponse;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public final class CoprocessorService {
    private static final Logger log = LoggerFactory.getLogger(CoprocessorService.class);

    private final Map<Integer, Coprocessor> handlers = new ConcurrentHashMap<>();

    public void register(Coprocessor handler) {
        handlers.put(handler.requestType(), handler);
    }

    public Response handle(Request req) {
        var handler = handlers.get((int) req.getTp());
        if (handler == null) {
            return Response.newBuilder()
                    .setOtherError("unsupported coprocessor request type: " + req.getTp())
                    .build();
        }
        return handler.handle(req);
    }

    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        var handler = handlers.get((int) req.getTp());
        if (handler == null) {
            sink.accept(StreamResponse.newBuilder()
                    .setOtherError("unsupported coprocessor request type: " + req.getTp())
                    .build());
            return;
        }
        handler.handleStream(req, sink);
    }

    public void handleBatch(BatchRequest req, Consumer<BatchResponse> sink) {
        for (int i = 0; i < req.getRegionsCount(); i++) {
            var region = req.getRegions(i);
            var singleReq = Request.newBuilder()
                    .setTp(req.getTp())
                    .setData(req.getData())
                    .setStartTs(req.getStartTs())
                    .addAllRanges(region.getRangesList())
                    .build();
            try {
                var resp = handle(singleReq);
                sink.accept(BatchResponse.newBuilder()
                        .setData(resp.getData())
                        .setRegionId(region.getRegionId())
                        .build());
            } catch (Throwable t) {
                log.warn("batch coprocessor region={} failed", region.getRegionId(), t);
                sink.accept(BatchResponse.newBuilder()
                        .setOtherError(t.getMessage())
                        .setRegionId(region.getRegionId())
                        .build());
            }
        }
    }
}
