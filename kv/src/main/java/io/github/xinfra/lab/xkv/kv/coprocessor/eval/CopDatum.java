package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public sealed interface CopDatum permits
        CopDatum.IntVal, CopDatum.DoubleVal, CopDatum.DecimalVal,
        CopDatum.StringVal, CopDatum.BytesVal, CopDatum.DateTimeVal,
        CopDatum.NullVal {

    record IntVal(long value) implements CopDatum {
        @Override public String toString() { return Long.toString(value); }
    }

    record DoubleVal(double value) implements CopDatum {
        @Override public String toString() { return Double.toString(value); }
    }

    record DecimalVal(BigDecimal value) implements CopDatum {
        @Override public String toString() { return value.toPlainString(); }
    }

    record StringVal(String value) implements CopDatum {
        @Override public String toString() { return value; }
    }

    record BytesVal(byte[] value) implements CopDatum {
        @Override public boolean equals(Object o) {
            return o instanceof BytesVal b && Arrays.equals(value, b.value);
        }
        @Override public int hashCode() { return Arrays.hashCode(value); }
        @Override public String toString() {
            StringBuilder sb = new StringBuilder("0x");
            for (byte b : value) sb.append(String.format("%02x", b & 0xFF));
            return sb.toString();
        }
    }

    record DateTimeVal(LocalDateTime value) implements CopDatum {
        @Override public String toString() {
            return value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
    }

    record NullVal() implements CopDatum {
        @Override public String toString() { return "NULL"; }
    }

    NullVal NULL_INSTANCE = new NullVal();

    static CopDatum of(long v) { return new IntVal(v); }
    static CopDatum of(double v) { return new DoubleVal(v); }
    static CopDatum of(BigDecimal v) { return v == null ? nil() : new DecimalVal(v); }
    static CopDatum of(String v) { return v == null ? nil() : new StringVal(v); }
    static CopDatum of(byte[] v) { return v == null ? nil() : new BytesVal(v); }
    static CopDatum of(LocalDateTime v) { return v == null ? nil() : new DateTimeVal(v); }
    static CopDatum nil() { return NULL_INSTANCE; }

    default boolean isNull() { return this instanceof NullVal; }

    default long toLong() {
        if (this instanceof IntVal d) return d.value;
        if (this instanceof DoubleVal d) return (long) d.value;
        if (this instanceof DecimalVal d) return d.value.longValue();
        if (this instanceof StringVal d) {
            try { return Long.parseLong(d.value.trim()); }
            catch (NumberFormatException e) { return 0L; }
        }
        return 0L;
    }

    default double toDouble() {
        if (this instanceof IntVal d) return (double) d.value;
        if (this instanceof DoubleVal d) return d.value;
        if (this instanceof DecimalVal d) return d.value.doubleValue();
        if (this instanceof StringVal d) {
            try { return Double.parseDouble(d.value.trim()); }
            catch (NumberFormatException e) { return 0.0; }
        }
        return 0.0;
    }

    default String toStringValue() { return toString(); }

    default boolean toBoolean() {
        if (this instanceof NullVal) return false;
        if (this instanceof IntVal d) return d.value != 0;
        if (this instanceof DoubleVal d) return d.value != 0.0;
        if (this instanceof DecimalVal d) return d.value.signum() != 0;
        if (this instanceof StringVal d) return !d.value.isEmpty() && !"0".equals(d.value);
        return true;
    }
}
