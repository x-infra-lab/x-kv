package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import io.github.xinfra.lab.xkv.proto.Tipb;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public final class ExprEvaluator {
    private ExprEvaluator() {}

    public static CopDatum eval(Tipb.Expr expr, CopRow row) {
        return switch (expr.getTp()) {
            case CONSTANT -> fromProto(expr.getVal());
            case COLUMN_REF -> row.get(expr.getColumnIndex());
            case BINARY_OP -> evalBinaryOp(expr, row);
            case UNARY_OP -> evalUnaryOp(expr, row);
            case LIKE -> evalLike(expr, row);
            case IN -> evalIn(expr, row);
            case BETWEEN -> evalBetween(expr, row);
            case CAST -> evalCast(expr, row);
            case CASE_WHEN -> evalCaseWhen(expr, row);
            case FUNCTION_CALL -> evalFunction(expr, row);
            default -> throw new IllegalArgumentException("Unknown expr type: " + expr.getTp());
        };
    }

    public static boolean passesFilter(CopRow row, List<Tipb.Expr> conditions) {
        if (conditions == null || conditions.isEmpty()) return true;
        for (Tipb.Expr cond : conditions) {
            CopDatum result = eval(cond, row);
            if (!result.toBoolean()) return false;
        }
        return true;
    }

    public static CopDatum fromProto(Tipb.Datum datum) {
        return switch (datum.getValueCase()) {
            case INT_VAL -> CopDatum.of(datum.getIntVal());
            case DOUBLE_VAL -> CopDatum.of(datum.getDoubleVal());
            case DECIMAL_VAL -> CopDatum.of(new BigDecimal(datum.getDecimalVal()));
            case STRING_VAL -> CopDatum.of(datum.getStringVal());
            case BYTES_VAL -> CopDatum.of(datum.getBytesVal().toByteArray());
            case DATETIME_VAL -> CopDatum.of(LocalDateTime.parse(datum.getDatetimeVal(),
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            case NULL_VAL -> CopDatum.nil();
            case VALUE_NOT_SET -> CopDatum.nil();
        };
    }

    public static Tipb.Datum toProto(CopDatum datum) {
        Tipb.Datum.Builder b = Tipb.Datum.newBuilder();
        switch (datum) {
            case CopDatum.IntVal d -> b.setIntVal(d.value());
            case CopDatum.DoubleVal d -> b.setDoubleVal(d.value());
            case CopDatum.DecimalVal d -> b.setDecimalVal(d.value().toPlainString());
            case CopDatum.StringVal d -> b.setStringVal(d.value());
            case CopDatum.BytesVal d -> b.setBytesVal(com.google.protobuf.ByteString.copyFrom(d.value()));
            case CopDatum.DateTimeVal d -> b.setDatetimeVal(
                    d.value().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            case CopDatum.NullVal n -> b.setNullVal(true);
        }
        return b.build();
    }

    // --- BinaryOp ---

    private static CopDatum evalBinaryOp(Tipb.Expr expr, CopRow row) {
        int op = expr.getOp();
        CopDatum lv = eval(expr.getChildren(0), row);

        // AND (op=12): short-circuit
        if (op == 12) {
            if (!lv.isNull() && !lv.toBoolean()) return CopDatum.of(0L);
            CopDatum rv = eval(expr.getChildren(1), row);
            if (!rv.isNull() && !rv.toBoolean()) return CopDatum.of(0L);
            if (lv.isNull() || rv.isNull()) return CopDatum.nil();
            return CopDatum.of(1L);
        }
        // OR (op=13): short-circuit
        if (op == 13) {
            if (!lv.isNull() && lv.toBoolean()) return CopDatum.of(1L);
            CopDatum rv = eval(expr.getChildren(1), row);
            if (!rv.isNull() && rv.toBoolean()) return CopDatum.of(1L);
            if (lv.isNull() || rv.isNull()) return CopDatum.nil();
            return CopDatum.of(0L);
        }

        CopDatum rv = eval(expr.getChildren(1), row);
        if (lv.isNull() || rv.isNull()) return CopDatum.nil();

        // Comparison ops: EQ=6, NE=7, LT=8, LE=9, GT=10, GE=11
        if (op >= 6 && op <= 11) {
            int cmp = CopDatumComparator.compare(lv, rv);
            boolean result = switch (op) {
                case 6 -> cmp == 0;
                case 7 -> cmp != 0;
                case 8 -> cmp < 0;
                case 9 -> cmp <= 0;
                case 10 -> cmp > 0;
                case 11 -> cmp >= 0;
                default -> false;
            };
            return CopDatum.of(result ? 1L : 0L);
        }

        // Arithmetic ops
        return switch (op) {
            case 0 -> addDatum(lv, rv);   // ADD
            case 1 -> subDatum(lv, rv);   // SUB
            case 2 -> mulDatum(lv, rv);   // MUL
            case 3 -> divDatum(lv, rv);   // DIV
            case 4 -> modDatum(lv, rv);   // MOD
            case 5 -> {                   // INT_DIV
                long d = rv.toLong();
                if (d == 0) yield CopDatum.nil();
                long n = lv.toLong();
                if (n == Long.MIN_VALUE && d == -1) throw new ArithmeticException("long overflow");
                yield CopDatum.of(n / d);
            }
            default -> throw new IllegalArgumentException("Unknown binary op: " + op);
        };
    }

    private static CopDatum addDatum(CopDatum a, CopDatum b) {
        if (a instanceof CopDatum.IntVal && b instanceof CopDatum.IntVal)
            return CopDatum.of(Math.addExact(a.toLong(), b.toLong()));
        return CopDatum.of(a.toDouble() + b.toDouble());
    }

    private static CopDatum subDatum(CopDatum a, CopDatum b) {
        if (a instanceof CopDatum.IntVal && b instanceof CopDatum.IntVal)
            return CopDatum.of(Math.subtractExact(a.toLong(), b.toLong()));
        return CopDatum.of(a.toDouble() - b.toDouble());
    }

    private static CopDatum mulDatum(CopDatum a, CopDatum b) {
        if (a instanceof CopDatum.IntVal && b instanceof CopDatum.IntVal)
            return CopDatum.of(Math.multiplyExact(a.toLong(), b.toLong()));
        return CopDatum.of(a.toDouble() * b.toDouble());
    }

    private static CopDatum divDatum(CopDatum a, CopDatum b) {
        double dv = b.toDouble();
        if (dv == 0.0) return CopDatum.nil();
        return CopDatum.of(a.toDouble() / dv);
    }

    private static CopDatum modDatum(CopDatum a, CopDatum b) {
        if (a instanceof CopDatum.IntVal && b instanceof CopDatum.IntVal) {
            long bv = b.toLong();
            if (bv == 0) return CopDatum.nil();
            return CopDatum.of(a.toLong() % bv);
        }
        double bv = b.toDouble();
        if (bv == 0.0) return CopDatum.nil();
        return CopDatum.of(a.toDouble() % bv);
    }

    // --- UnaryOp ---
    // Op ordinals: NOT=0, NEG=1, IS_NULL=2, IS_NOT_NULL=3

    private static CopDatum evalUnaryOp(Tipb.Expr expr, CopRow row) {
        CopDatum v = eval(expr.getChildren(0), row);
        int op = expr.getOp();
        if (op == 2) return CopDatum.of(v.isNull() ? 1L : 0L);      // IS_NULL
        if (op == 3) return CopDatum.of(v.isNull() ? 0L : 1L);      // IS_NOT_NULL
        if (v.isNull()) return CopDatum.nil();
        if (op == 0) return CopDatum.of(v.toBoolean() ? 0L : 1L);   // NOT
        // NEG (op=1)
        if (v instanceof CopDatum.IntVal i) {
            try {
                return CopDatum.of(Math.negateExact(i.value()));
            } catch (ArithmeticException e) {
                return CopDatum.of(-(double) i.value());
            }
        }
        return CopDatum.of(-v.toDouble());
    }

    // --- LIKE ---

    private static CopDatum evalLike(Tipb.Expr expr, CopRow row) {
        CopDatum v = eval(expr.getChildren(0), row);
        CopDatum p = eval(expr.getChildren(1), row);
        if (v.isNull() || p.isNull()) return CopDatum.nil();
        boolean matches = likeMatch(v.toStringValue(), p.toStringValue());
        return CopDatum.of((matches ^ expr.getNot()) ? 1L : 0L);
    }

    static boolean likeMatch(String str, String pat) {
        int si = 0, pi = 0;
        int sLen = str.length(), pLen = pat.length();
        int starPi = -1, starSi = -1;

        while (si < sLen) {
            if (pi < pLen && pat.charAt(pi) == '%') {
                starPi = pi; starSi = si; pi++;
            } else if (pi < pLen && pat.charAt(pi) == '\\' && pi + 1 < pLen) {
                if (Character.toLowerCase(str.charAt(si)) == Character.toLowerCase(pat.charAt(pi + 1))) {
                    si++; pi += 2;
                } else if (starPi != -1) {
                    pi = starPi + 1; starSi++; si = starSi;
                } else {
                    return false;
                }
            } else if (pi < pLen && (pat.charAt(pi) == '_'
                    || Character.toLowerCase(str.charAt(si)) == Character.toLowerCase(pat.charAt(pi)))) {
                si++; pi++;
            } else if (starPi != -1) {
                pi = starPi + 1; starSi++; si = starSi;
            } else {
                return false;
            }
        }

        while (pi < pLen && pat.charAt(pi) == '%') pi++;
        return pi == pLen;
    }

    // --- IN ---

    private static CopDatum evalIn(Tipb.Expr expr, CopRow row) {
        CopDatum v = eval(expr.getChildren(0), row);
        if (v.isNull()) return CopDatum.nil();
        boolean found = false;
        boolean hasNull = false;
        for (int i = 1; i < expr.getChildrenCount(); i++) {
            CopDatum item = eval(expr.getChildren(i), row);
            if (item.isNull()) { hasNull = true; continue; }
            if (CopDatumComparator.compare(v, item) == 0) { found = true; break; }
        }
        if (found) return CopDatum.of(expr.getNot() ? 0L : 1L);
        if (hasNull) return CopDatum.nil();
        return CopDatum.of(expr.getNot() ? 1L : 0L);
    }

    // --- BETWEEN ---

    private static CopDatum evalBetween(Tipb.Expr expr, CopRow row) {
        CopDatum v = eval(expr.getChildren(0), row);
        CopDatum lo = eval(expr.getChildren(1), row);
        CopDatum hi = eval(expr.getChildren(2), row);
        if (v.isNull() || lo.isNull() || hi.isNull()) return CopDatum.nil();
        boolean between = CopDatumComparator.compare(v, lo) >= 0
                && CopDatumComparator.compare(v, hi) <= 0;
        return CopDatum.of((between ^ expr.getNot()) ? 1L : 0L);
    }

    // --- CAST ---

    private static CopDatum evalCast(Tipb.Expr expr, CopRow row) {
        CopDatum v = eval(expr.getChildren(0), row);
        if (v.isNull()) return CopDatum.nil();
        int targetType = expr.getDataType();
        return coerce(v, targetType);
    }

    static CopDatum coerce(CopDatum value, int targetType) {
        if (value.isNull()) return value;
        // Integer types: TINYINT=0,SMALLINT=1,INT=2,BIGINT=3
        if (targetType >= 0 && targetType <= 3) return CopDatum.of(value.toLong());
        // FLOAT=4, DOUBLE=5
        if (targetType == 4 || targetType == 5) return CopDatum.of(value.toDouble());
        // DECIMAL=6
        if (targetType == 6) {
            if (value instanceof CopDatum.DecimalVal) return value;
            if (value instanceof CopDatum.IntVal i) return CopDatum.of(BigDecimal.valueOf(i.value()));
            if (value instanceof CopDatum.DoubleVal d) return CopDatum.of(BigDecimal.valueOf(d.value()));
            try { return CopDatum.of(new BigDecimal(value.toStringValue())); }
            catch (NumberFormatException e) { return CopDatum.nil(); }
        }
        // String types: CHAR=7,VARCHAR=8,TEXT=9
        if (targetType >= 7 && targetType <= 9) return CopDatum.of(value.toStringValue());
        // DATETIME=13, TIMESTAMP=14
        if (targetType == 13 || targetType == 14) {
            if (value instanceof CopDatum.DateTimeVal) return value;
            try {
                return CopDatum.of(LocalDateTime.parse(value.toStringValue().trim(),
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            } catch (DateTimeParseException e) { return CopDatum.nil(); }
        }
        return value;
    }

    // --- CASE WHEN ---

    private static CopDatum evalCaseWhen(Tipb.Expr expr, CopRow row) {
        int idx = 0;
        boolean hasCompare = expr.getChildrenCount() > 0 && expr.getOp() == 1;
        CopDatum cmpVal = null;
        if (hasCompare) {
            cmpVal = eval(expr.getChildren(0), row);
            idx = 1;
        }

        while (idx + 1 < expr.getChildrenCount()) {
            CopDatum cond = eval(expr.getChildren(idx), row);
            if (hasCompare) {
                if (!cmpVal.isNull() && !cond.isNull()
                        && CopDatumComparator.compare(cmpVal, cond) == 0) {
                    return eval(expr.getChildren(idx + 1), row);
                }
            } else {
                if (!cond.isNull() && cond.toBoolean()) {
                    return eval(expr.getChildren(idx + 1), row);
                }
            }
            idx += 2;
        }

        // else clause
        if (idx < expr.getChildrenCount()) {
            return eval(expr.getChildren(idx), row);
        }
        return CopDatum.nil();
    }

    // --- FUNCTION_CALL ---

    private static CopDatum evalFunction(Tipb.Expr expr, CopRow row) {
        String name = expr.getFuncName().toUpperCase();
        List<CopDatum> args = new ArrayList<>(expr.getChildrenCount());
        for (Tipb.Expr child : expr.getChildrenList()) {
            args.add(eval(child, row));
        }
        return evalScalarFunction(name, args);
    }

    private static CopDatum evalScalarFunction(String name, List<CopDatum> args) {
        return switch (name) {
            case "IF" -> args.size() >= 3 ? (args.get(0).toBoolean() ? args.get(1) : args.get(2)) : CopDatum.nil();
            case "IFNULL" -> args.size() >= 2 ? (args.get(0).isNull() ? args.get(1) : args.get(0)) : CopDatum.nil();
            case "NULLIF" -> args.size() >= 2 && CopDatumComparator.compare(args.get(0), args.get(1)) == 0
                    ? CopDatum.nil() : (args.isEmpty() ? CopDatum.nil() : args.get(0));
            case "COALESCE" -> {
                for (CopDatum d : args) if (!d.isNull()) yield d;
                yield CopDatum.nil();
            }
            case "CONCAT" -> {
                StringBuilder sb = new StringBuilder();
                for (CopDatum d : args) { if (d.isNull()) yield CopDatum.nil(); sb.append(d.toStringValue()); }
                yield CopDatum.of(sb.toString());
            }
            case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().length()));
            case "UPPER", "UCASE" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().toUpperCase()));
            case "LOWER", "LCASE" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().toLowerCase()));
            case "TRIM" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().trim()));
            case "ABS" -> nullSafe1(args, d -> {
                if (d instanceof CopDatum.IntVal i) {
                    long v = i.value();
                    if (v == Long.MIN_VALUE) return CopDatum.of(-(double) v);
                    return CopDatum.of(Math.abs(v));
                }
                return CopDatum.of(Math.abs(d.toDouble()));
            });
            case "CEIL", "CEILING" -> nullSafe1(args, d -> CopDatum.of((long) Math.ceil(d.toDouble())));
            case "FLOOR" -> nullSafe1(args, d -> CopDatum.of((long) Math.floor(d.toDouble())));
            case "ROUND" -> {
                if (args.isEmpty() || args.get(0).isNull()) yield CopDatum.nil();
                double v = args.get(0).toDouble();
                int scale = args.size() >= 2 ? (int) args.get(1).toLong() : 0;
                BigDecimal bd = BigDecimal.valueOf(v).setScale(scale, RoundingMode.HALF_UP);
                yield scale == 0 ? CopDatum.of(bd.longValue()) : CopDatum.of(bd.doubleValue());
            }
            case "MOD" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                long b = args.get(1).toLong();
                yield b == 0 ? CopDatum.nil() : CopDatum.of(args.get(0).toLong() % b);
            }
            case "NOW", "CURRENT_TIMESTAMP" -> CopDatum.of(LocalDateTime.now());
            case "VERSION" -> CopDatum.of("8.0.30-x-db");
            default -> throw new UnsupportedOperationException("Unknown function: " + name);
        };
    }

    @FunctionalInterface
    private interface DatumMapper { CopDatum apply(CopDatum d); }

    private static CopDatum nullSafe1(List<CopDatum> args, DatumMapper fn) {
        if (args.isEmpty() || args.get(0).isNull()) return CopDatum.nil();
        return fn.apply(args.get(0));
    }
}
