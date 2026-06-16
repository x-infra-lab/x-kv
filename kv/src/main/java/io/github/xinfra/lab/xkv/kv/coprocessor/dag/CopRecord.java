package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;

public record CopRecord(byte[] key, byte[] value, CopRow row) {}
