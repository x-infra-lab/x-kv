package io.github.xinfra.lab.xkv.kv.cdc;

import io.github.xinfra.lab.xkv.proto.Cdcpb;

public record CdcEvent(
        long regionId,
        Cdcpb.Row.OpType type,
        byte[] key,
        byte[] value,
        byte[] oldValue,
        long startTs,
        long commitTs
) {}
