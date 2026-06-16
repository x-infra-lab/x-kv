package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.Set;

public sealed interface CopAggFunction permits
        CopAggFunction.CountAgg, CopAggFunction.SumAgg, CopAggFunction.AvgAgg,
        CopAggFunction.MinAgg, CopAggFunction.MaxAgg, CopAggFunction.GroupConcatAgg {

    // Type ordinals match AggFunction.Type: COUNT=0, SUM=1, AVG=2, MIN=3, MAX=4, GROUP_CONCAT=5
    int type();
    boolean distinct();
    void update(CopDatum value);
    void merge(CopAggFunction other);
    CopDatum result();
    long partialCount();
    BigDecimal partialSum();
    CopAggFunction newInstance();

    static CopAggFunction create(int type, boolean distinct) {
        return switch (type) {
            case 0 -> new CountAgg(distinct);
            case 1 -> new SumAgg(distinct);
            case 2 -> new AvgAgg(distinct);
            case 3 -> new MinAgg(distinct);
            case 4 -> new MaxAgg(distinct);
            case 5 -> new GroupConcatAgg(distinct);
            default -> throw new IllegalArgumentException("Unknown agg type: " + type);
        };
    }

    static void restorePartialState(CopAggFunction agg, int type, java.util.List<CopDatum> state) {
        switch (type) {
            case 0 -> { // COUNT
                if (agg instanceof CountAgg c) c.count = state.get(0).toLong();
            }
            case 1 -> { // SUM
                if (agg instanceof SumAgg s) {
                    CopDatum val = state.get(0);
                    if (!val.isNull()) {
                        s.hasValue = true;
                        s.sum = toBigDecimal(val);
                    }
                }
            }
            case 2 -> { // AVG
                if (agg instanceof AvgAgg a) {
                    a.count = state.get(0).toLong();
                    CopDatum sumVal = state.get(1);
                    if (!sumVal.isNull()) a.sum = toBigDecimal(sumVal);
                }
            }
            case 3, 4 -> { // MIN, MAX
                CopDatum val = state.get(0);
                if (!val.isNull()) agg.update(val);
            }
            case 5 -> { // GROUP_CONCAT
                CopDatum val = state.get(0);
                if (!val.isNull()) agg.update(val);
            }
        }
    }

    private static BigDecimal toBigDecimal(CopDatum d) {
        if (d instanceof CopDatum.DecimalVal dd) return dd.value();
        if (d instanceof CopDatum.IntVal id) return BigDecimal.valueOf(id.value());
        return BigDecimal.valueOf(d.toDouble());
    }

    // --- Implementations ---

    final class CountAgg implements CopAggFunction {
        private final boolean dist;
        long count;
        private final Set<String> seen;

        CountAgg(boolean distinct) { this.dist = distinct; this.seen = distinct ? new HashSet<>() : null; }

        @Override public int type() { return 0; }
        @Override public boolean distinct() { return dist; }
        @Override public void update(CopDatum value) {
            if (value.isNull()) return;
            if (dist && !seen.add(value.toStringValue())) return;
            count++;
        }
        @Override public void merge(CopAggFunction other) { count += ((CountAgg) other).count; }
        @Override public CopDatum result() { return CopDatum.of(count); }
        @Override public long partialCount() { return count; }
        @Override public BigDecimal partialSum() { return BigDecimal.ZERO; }
        @Override public CopAggFunction newInstance() { return new CountAgg(dist); }
    }

    final class SumAgg implements CopAggFunction {
        private final boolean dist;
        BigDecimal sum = BigDecimal.ZERO;
        boolean hasValue;
        private final Set<String> seen;

        SumAgg(boolean distinct) { this.dist = distinct; this.seen = distinct ? new HashSet<>() : null; }

        @Override public int type() { return 1; }
        @Override public boolean distinct() { return dist; }
        @Override public void update(CopDatum value) {
            if (value.isNull()) return;
            if (dist && !seen.add(value.toStringValue())) return;
            hasValue = true;
            if (value instanceof CopDatum.IntVal i) sum = sum.add(BigDecimal.valueOf(i.value()));
            else if (value instanceof CopDatum.DecimalVal d) sum = sum.add(d.value());
            else sum = sum.add(BigDecimal.valueOf(value.toDouble()));
        }
        @Override public void merge(CopAggFunction other) {
            SumAgg o = (SumAgg) other;
            if (o.hasValue) { hasValue = true; sum = sum.add(o.sum); }
        }
        @Override public CopDatum result() { return hasValue ? CopDatum.of(sum) : CopDatum.nil(); }
        @Override public long partialCount() { return 0; }
        @Override public BigDecimal partialSum() { return sum; }
        @Override public CopAggFunction newInstance() { return new SumAgg(dist); }
    }

    final class AvgAgg implements CopAggFunction {
        private final boolean dist;
        BigDecimal sum = BigDecimal.ZERO;
        long count;
        private final Set<String> seen;

        AvgAgg(boolean distinct) { this.dist = distinct; this.seen = distinct ? new HashSet<>() : null; }

        @Override public int type() { return 2; }
        @Override public boolean distinct() { return dist; }
        @Override public void update(CopDatum value) {
            if (value.isNull()) return;
            if (dist && !seen.add(value.toStringValue())) return;
            count++;
            if (value instanceof CopDatum.IntVal i) sum = sum.add(BigDecimal.valueOf(i.value()));
            else if (value instanceof CopDatum.DecimalVal d) sum = sum.add(d.value());
            else sum = sum.add(BigDecimal.valueOf(value.toDouble()));
        }
        @Override public void merge(CopAggFunction other) {
            AvgAgg o = (AvgAgg) other;
            sum = sum.add(o.sum);
            count += o.count;
        }
        @Override public CopDatum result() {
            return count > 0 ? CopDatum.of(sum.divide(BigDecimal.valueOf(count), 4, RoundingMode.HALF_UP)) : CopDatum.nil();
        }
        @Override public long partialCount() { return count; }
        @Override public BigDecimal partialSum() { return sum; }
        @Override public CopAggFunction newInstance() { return new AvgAgg(dist); }
    }

    final class MinAgg implements CopAggFunction {
        private final boolean dist;
        private CopDatum min = CopDatum.nil();

        MinAgg(boolean distinct) { this.dist = distinct; }

        @Override public int type() { return 3; }
        @Override public boolean distinct() { return dist; }
        @Override public void update(CopDatum value) {
            if (value.isNull()) return;
            if (min.isNull() || CopDatumComparator.compare(value, min) < 0) min = value;
        }
        @Override public void merge(CopAggFunction other) { update(((MinAgg) other).min); }
        @Override public CopDatum result() { return min; }
        @Override public long partialCount() { return 0; }
        @Override public BigDecimal partialSum() { return BigDecimal.ZERO; }
        @Override public CopAggFunction newInstance() { return new MinAgg(dist); }
    }

    final class MaxAgg implements CopAggFunction {
        private final boolean dist;
        private CopDatum max = CopDatum.nil();

        MaxAgg(boolean distinct) { this.dist = distinct; }

        @Override public int type() { return 4; }
        @Override public boolean distinct() { return dist; }
        @Override public void update(CopDatum value) {
            if (value.isNull()) return;
            if (max.isNull() || CopDatumComparator.compare(value, max) > 0) max = value;
        }
        @Override public void merge(CopAggFunction other) { update(((MaxAgg) other).max); }
        @Override public CopDatum result() { return max; }
        @Override public long partialCount() { return 0; }
        @Override public BigDecimal partialSum() { return BigDecimal.ZERO; }
        @Override public CopAggFunction newInstance() { return new MaxAgg(dist); }
    }

    final class GroupConcatAgg implements CopAggFunction {
        private final boolean dist;
        private final StringBuilder sb = new StringBuilder();
        private boolean first = true;
        private final Set<String> seen;

        GroupConcatAgg(boolean distinct) { this.dist = distinct; this.seen = distinct ? new HashSet<>() : null; }

        @Override public int type() { return 5; }
        @Override public boolean distinct() { return dist; }
        @Override public void update(CopDatum value) {
            if (value.isNull()) return;
            if (dist && !seen.add(value.toStringValue())) return;
            if (!first) sb.append(",");
            sb.append(value.toStringValue());
            first = false;
        }
        @Override public void merge(CopAggFunction other) {
            GroupConcatAgg o = (GroupConcatAgg) other;
            if (!o.first) {
                if (!first) sb.append(",");
                sb.append(o.sb);
                first = false;
            }
        }
        @Override public CopDatum result() { return first ? CopDatum.nil() : CopDatum.of(sb.toString()); }
        @Override public long partialCount() { return 0; }
        @Override public BigDecimal partialSum() { return BigDecimal.ZERO; }
        @Override public CopAggFunction newInstance() { return new GroupConcatAgg(dist); }
    }
}
