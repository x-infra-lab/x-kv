package io.github.xinfra.lab.xkv.client.cop;

import io.github.xinfra.lab.xkv.proto.Coprocessor;

import java.util.Iterator;

public interface CopClient extends AutoCloseable {

    Coprocessor.Response send(Coprocessor.Request request, long regionId);

    Iterator<RegionCopResult> sendToRangeParallel(
            int tp, byte[] data, long startTs,
            byte[] startKey, byte[] endKey, int concurrency);

    record RegionCopResult(long regionId, Coprocessor.Response response) {}
}
