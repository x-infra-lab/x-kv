package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import io.github.xinfra.lab.xkv.proto.Tipb;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CopRowDecoder {
    private static final byte ROW_FLAG = (byte) 0x80;

    private CopRowDecoder() {}

    public static CopRow decode(byte[] key, byte[] value, Tipb.DAGRequest dagReq) {
        long handle = CopKeyDecoder.decodeRowHandle(key);
        Map<Long, CopDatum> colValues = decodeRowValue(value);
        List<Tipb.ColumnInfo> columns = dagReq.getColumnsList();
        List<Integer> outputIndices = dagReq.getOutputColumnIndicesList();

        CopDatum[] values = new CopDatum[outputIndices.size()];
        for (int i = 0; i < outputIndices.size(); i++) {
            Tipb.ColumnInfo col = columns.get(outputIndices.get(i));
            CopDatum v = colValues.get(col.getColumnId());
            if (v != null) {
                values[i] = v;
            } else if (col.getAutoIncrement() || isPrimaryKeyColumn(col, dagReq)) {
                values[i] = CopDatum.of(handle);
            } else {
                values[i] = CopDatum.nil();
            }
        }
        return new CopRow(values);
    }

    private static boolean isPrimaryKeyColumn(Tipb.ColumnInfo col, Tipb.DAGRequest dagReq) {
        if (dagReq.getColumnsCount() == 0) return false;
        Tipb.ColumnInfo first = dagReq.getColumns(0);
        return first.getColumnId() == col.getColumnId() && first.getAutoIncrement();
    }

    public static Map<Long, CopDatum> decodeRowValue(byte[] data) {
        Map<Long, CopDatum> result = new HashMap<>();
        if (data == null || data.length == 0) return result;

        int offset = 0;
        if (data[0] == ROW_FLAG) offset = 1;

        int[] bytesRead = new int[1];
        while (offset < data.length) {
            long colId = CopCodecUtil.decodeUvarint(data, offset, bytesRead);
            offset += bytesRead[0];

            CopDatum value = decodeValue(data, offset, bytesRead);
            offset += bytesRead[0];

            result.put(colId, value);
        }

        return result;
    }

    private static CopDatum decodeValue(byte[] data, int offset, int[] bytesRead) {
        byte flag = data[offset];
        int pos = offset + 1;

        return switch (flag) {
            case CopCodecUtil.NULL_FLAG -> { bytesRead[0] = 1; yield CopDatum.nil(); }
            case CopCodecUtil.INT_FLAG -> {
                int[] innerRead = new int[1];
                long v = CopCodecUtil.decodeVarint(data, pos, innerRead);
                bytesRead[0] = 1 + innerRead[0];
                yield CopDatum.of(v);
            }
            case CopCodecUtil.FLOAT_FLAG -> {
                long bits = CopCodecUtil.decodeUint64(data, pos);
                bytesRead[0] = 9;
                yield CopDatum.of(Double.longBitsToDouble(bits));
            }
            case CopCodecUtil.COMPACT_BYTES_FLAG -> {
                int[] innerRead = new int[1];
                long len = CopCodecUtil.decodeVarint(data, pos, innerRead);
                pos += innerRead[0];
                byte[] raw = new byte[(int) len];
                System.arraycopy(data, pos, raw, 0, (int) len);
                bytesRead[0] = 1 + innerRead[0] + (int) len;
                yield CopDatum.of(new String(raw, StandardCharsets.UTF_8));
            }
            case CopCodecUtil.BYTES_DATUM_FLAG -> {
                int[] innerRead = new int[1];
                long len = CopCodecUtil.decodeVarint(data, pos, innerRead);
                pos += innerRead[0];
                byte[] raw = new byte[(int) len];
                System.arraycopy(data, pos, raw, 0, (int) len);
                bytesRead[0] = 1 + innerRead[0] + (int) len;
                yield CopDatum.of(raw);
            }
            case CopCodecUtil.DECIMAL_FLAG -> {
                int[] innerRead = new int[1];
                long len = CopCodecUtil.decodeVarint(data, pos, innerRead);
                pos += innerRead[0];
                byte[] raw = new byte[(int) len];
                System.arraycopy(data, pos, raw, 0, (int) len);
                bytesRead[0] = 1 + innerRead[0] + (int) len;
                yield CopDatum.of(new BigDecimal(new String(raw, StandardCharsets.UTF_8)));
            }
            case CopCodecUtil.DURATION_FLAG -> {
                bytesRead[0] = 9;
                yield CopDatum.of(CopCodecUtil.decodeDatetime(data, pos));
            }
            default -> throw new IllegalArgumentException("Unknown value flag: " + flag);
        };
    }
}
