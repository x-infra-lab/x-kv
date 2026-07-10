package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import java.math.BigDecimal;
import java.util.Arrays;

public final class CopDatumComparator {
    private CopDatumComparator() {}

    public static final int COLLATION_BINARY = 0;
    public static final int COLLATION_UTF8_GENERAL_CI = 1;
    public static final int COLLATION_UTF8_UNICODE_CI = 2;

    private static final ThreadLocal<Integer> COLLATION = ThreadLocal.withInitial(() -> COLLATION_BINARY);

    public static void setCollation(int collation) { COLLATION.set(collation); }
    public static int getCollation() { return COLLATION.get(); }
    public static void clearCollation() { COLLATION.remove(); }

    public static int compare(CopDatum a, CopDatum b) {
        if (a.isNull() && b.isNull()) return 0;
        if (a.isNull()) return -1;
        if (b.isNull()) return 1;

        if (a instanceof CopDatum.IntVal ai && b instanceof CopDatum.IntVal bi)
            return Long.compare(ai.value(), bi.value());
        if (a instanceof CopDatum.DoubleVal ad && b instanceof CopDatum.DoubleVal bd)
            return Double.compare(ad.value(), bd.value());
        if (a instanceof CopDatum.DecimalVal ad && b instanceof CopDatum.DecimalVal bd)
            return ad.value().compareTo(bd.value());
        if (a instanceof CopDatum.StringVal as && b instanceof CopDatum.StringVal bs)
            return compareStrings(as.value(), bs.value());
        if (a instanceof CopDatum.DateTimeVal ad && b instanceof CopDatum.DateTimeVal bd)
            return ad.value().compareTo(bd.value());
        if (a instanceof CopDatum.BytesVal ab && b instanceof CopDatum.BytesVal bb)
            return Arrays.compareUnsigned(ab.value(), bb.value());

        if (isNumeric(a) && isNumeric(b)) {
            if (a instanceof CopDatum.DecimalVal || b instanceof CopDatum.DecimalVal) {
                return toDecimal(a).compareTo(toDecimal(b));
            }
            if (a instanceof CopDatum.DoubleVal || b instanceof CopDatum.DoubleVal) {
                return Double.compare(a.toDouble(), b.toDouble());
            }
            return Long.compare(a.toLong(), b.toLong());
        }

        return compareStrings(a.toStringValue(), b.toStringValue());
    }

    private static int compareStrings(String a, String b) {
        int collation = COLLATION.get();
        if (collation == COLLATION_UTF8_GENERAL_CI || collation == COLLATION_UTF8_UNICODE_CI) {
            return a.compareToIgnoreCase(b);
        }
        return a.compareTo(b);
    }

    private static boolean isNumeric(CopDatum d) {
        return d instanceof CopDatum.IntVal || d instanceof CopDatum.DoubleVal
                || d instanceof CopDatum.DecimalVal;
    }

    private static BigDecimal toDecimal(CopDatum d) {
        if (d instanceof CopDatum.DecimalVal dd) return dd.value();
        if (d instanceof CopDatum.IntVal di) return BigDecimal.valueOf(di.value());
        if (d instanceof CopDatum.DoubleVal dd) return BigDecimal.valueOf(dd.value());
        return new BigDecimal(d.toStringValue());
    }
}
