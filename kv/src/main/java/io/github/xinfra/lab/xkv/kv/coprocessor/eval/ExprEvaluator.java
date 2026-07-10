package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import io.github.xinfra.lab.xkv.proto.Tipb;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;

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
            // --- Control flow ---
            case "IF" -> args.size() >= 3 ? (args.get(0).toBoolean() ? args.get(1) : args.get(2)) : CopDatum.nil();
            case "IFNULL" -> args.size() >= 2 ? (args.get(0).isNull() ? args.get(1) : args.get(0)) : CopDatum.nil();
            case "NULLIF" -> args.size() >= 2 && CopDatumComparator.compare(args.get(0), args.get(1)) == 0
                    ? CopDatum.nil() : (args.isEmpty() ? CopDatum.nil() : args.get(0));
            case "COALESCE" -> {
                for (CopDatum d : args) if (!d.isNull()) yield d;
                yield CopDatum.nil();
            }

            // --- String functions ---
            case "CONCAT" -> {
                StringBuilder sb = new StringBuilder();
                for (CopDatum d : args) { if (d.isNull()) yield CopDatum.nil(); sb.append(d.toStringValue()); }
                yield CopDatum.of(sb.toString());
            }
            case "CONCAT_WS" -> {
                if (args.isEmpty() || args.get(0).isNull()) yield CopDatum.nil();
                String sep = args.get(0).toStringValue();
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (int i = 1; i < args.size(); i++) {
                    if (args.get(i).isNull()) continue;
                    if (!first) sb.append(sep);
                    sb.append(args.get(i).toStringValue());
                    first = false;
                }
                yield CopDatum.of(sb.toString());
            }
            case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().length()));
            case "UPPER", "UCASE" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().toUpperCase()));
            case "LOWER", "LCASE" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().toLowerCase()));
            case "TRIM" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().trim()));
            case "LTRIM" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().stripLeading()));
            case "RTRIM" -> nullSafe1(args, d -> CopDatum.of(d.toStringValue().stripTrailing()));
            case "REVERSE" -> nullSafe1(args, d -> CopDatum.of(new StringBuilder(d.toStringValue()).reverse().toString()));
            case "SPACE" -> nullSafe1(args, d -> {
                int n = (int) Math.min(d.toLong(), 65535);
                return CopDatum.of(" ".repeat(Math.max(0, n)));
            });
            case "REPEAT" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                int n = (int) Math.min(args.get(1).toLong(), 65535);
                yield n <= 0 ? CopDatum.of("") : CopDatum.of(args.get(0).toStringValue().repeat(n));
            }
            case "SUBSTR", "SUBSTRING", "MID" -> {
                if (args.isEmpty() || args.get(0).isNull()) yield CopDatum.nil();
                String s = args.get(0).toStringValue();
                int pos = args.size() >= 2 ? (int) args.get(1).toLong() : 1;
                int len = args.size() >= 3 ? (int) args.get(2).toLong() : s.length();
                if (pos > 0) pos--;
                else if (pos < 0) pos = s.length() + pos;
                else { yield CopDatum.of(""); }
                if (pos < 0) pos = 0;
                int end = Math.min(pos + len, s.length());
                yield pos >= s.length() ? CopDatum.of("") : CopDatum.of(s.substring(pos, end));
            }
            case "LEFT" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                String s = args.get(0).toStringValue();
                int n = (int) args.get(1).toLong();
                yield CopDatum.of(n >= s.length() ? s : s.substring(0, Math.max(0, n)));
            }
            case "RIGHT" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                String s = args.get(0).toStringValue();
                int n = (int) args.get(1).toLong();
                yield CopDatum.of(n >= s.length() ? s : s.substring(s.length() - n));
            }
            case "REPLACE" -> {
                if (args.size() < 3 || args.get(0).isNull()) yield CopDatum.nil();
                yield CopDatum.of(args.get(0).toStringValue()
                        .replace(args.get(1).toStringValue(), args.get(2).toStringValue()));
            }
            case "LPAD" -> {
                if (args.size() < 3 || args.get(0).isNull()) yield CopDatum.nil();
                String s = args.get(0).toStringValue();
                int targetLen = (int) args.get(1).toLong();
                String pad = args.get(2).toStringValue();
                if (pad.isEmpty() || targetLen < 0) yield CopDatum.nil();
                if (s.length() >= targetLen) yield CopDatum.of(s.substring(0, targetLen));
                StringBuilder sb = new StringBuilder();
                while (sb.length() + s.length() < targetLen) sb.append(pad);
                sb.append(s);
                yield CopDatum.of(sb.substring(sb.length() - targetLen));
            }
            case "RPAD" -> {
                if (args.size() < 3 || args.get(0).isNull()) yield CopDatum.nil();
                String s = args.get(0).toStringValue();
                int targetLen = (int) args.get(1).toLong();
                String pad = args.get(2).toStringValue();
                if (pad.isEmpty() || targetLen < 0) yield CopDatum.nil();
                if (s.length() >= targetLen) yield CopDatum.of(s.substring(0, targetLen));
                StringBuilder sb = new StringBuilder(s);
                while (sb.length() < targetLen) sb.append(pad);
                yield CopDatum.of(sb.substring(0, targetLen));
            }
            case "LOCATE", "INSTR", "POSITION" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                String substr = args.get(0).toStringValue();
                String str = args.get(1).toStringValue();
                int startPos = args.size() >= 3 ? (int) args.get(2).toLong() - 1 : 0;
                int idx = str.indexOf(substr, Math.max(0, startPos));
                yield CopDatum.of((long) (idx + 1));
            }
            case "HEX" -> nullSafe1(args, d -> {
                if (d instanceof CopDatum.IntVal i) return CopDatum.of(Long.toHexString(i.value()).toUpperCase());
                StringBuilder sb = new StringBuilder();
                for (byte b : d.toStringValue().getBytes()) sb.append(String.format("%02X", b & 0xFF));
                return CopDatum.of(sb.toString());
            });
            case "UNHEX" -> nullSafe1(args, d -> {
                String hex = d.toStringValue();
                if (hex.length() % 2 != 0) return CopDatum.nil();
                byte[] bytes = new byte[hex.length() / 2];
                for (int i = 0; i < bytes.length; i++) {
                    int hi = Character.digit(hex.charAt(i * 2), 16);
                    int lo = Character.digit(hex.charAt(i * 2 + 1), 16);
                    if (hi < 0 || lo < 0) return CopDatum.nil();
                    bytes[i] = (byte) ((hi << 4) | lo);
                }
                return CopDatum.of(new String(bytes));
            });

            // --- Math functions ---
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
            case "TRUNCATE" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                double v = args.get(0).toDouble();
                int scale = (int) args.get(1).toLong();
                BigDecimal bd = BigDecimal.valueOf(v).setScale(scale, RoundingMode.DOWN);
                yield scale == 0 ? CopDatum.of(bd.longValue()) : CopDatum.of(bd.doubleValue());
            }
            case "MOD" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                long b = args.get(1).toLong();
                yield b == 0 ? CopDatum.nil() : CopDatum.of(args.get(0).toLong() % b);
            }
            case "SQRT" -> nullSafe1(args, d -> {
                double v = d.toDouble();
                return v < 0 ? CopDatum.nil() : CopDatum.of(Math.sqrt(v));
            });
            case "POW", "POWER" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                yield CopDatum.of(Math.pow(args.get(0).toDouble(), args.get(1).toDouble()));
            }
            case "LOG" -> {
                if (args.isEmpty() || args.get(0).isNull()) yield CopDatum.nil();
                if (args.size() >= 2) {
                    if (args.get(1).isNull()) yield CopDatum.nil();
                    double base = args.get(0).toDouble();
                    double val = args.get(1).toDouble();
                    yield (base <= 0 || base == 1 || val <= 0) ? CopDatum.nil() : CopDatum.of(Math.log(val) / Math.log(base));
                }
                double val = args.get(0).toDouble();
                yield val <= 0 ? CopDatum.nil() : CopDatum.of(Math.log(val));
            }
            case "LOG2" -> nullSafe1(args, d -> {
                double v = d.toDouble();
                return v <= 0 ? CopDatum.nil() : CopDatum.of(Math.log(v) / Math.log(2));
            });
            case "LOG10", "LOG_10" -> nullSafe1(args, d -> {
                double v = d.toDouble();
                return v <= 0 ? CopDatum.nil() : CopDatum.of(Math.log10(v));
            });
            case "EXP" -> nullSafe1(args, d -> CopDatum.of(Math.exp(d.toDouble())));
            case "SIGN" -> nullSafe1(args, d -> {
                double v = d.toDouble();
                return CopDatum.of(v > 0 ? 1L : v < 0 ? -1L : 0L);
            });
            case "PI" -> CopDatum.of(Math.PI);
            case "RAND" -> CopDatum.of(ThreadLocalRandom.current().nextDouble());
            case "CRC32" -> nullSafe1(args, d -> {
                CRC32 crc = new CRC32();
                crc.update(d.toStringValue().getBytes());
                return CopDatum.of(crc.getValue());
            });
            case "GREATEST" -> {
                CopDatum max = null;
                for (CopDatum d : args) {
                    if (d.isNull()) yield CopDatum.nil();
                    if (max == null || CopDatumComparator.compare(d, max) > 0) max = d;
                }
                yield max == null ? CopDatum.nil() : max;
            }
            case "LEAST" -> {
                CopDatum min = null;
                for (CopDatum d : args) {
                    if (d.isNull()) yield CopDatum.nil();
                    if (min == null || CopDatumComparator.compare(d, min) < 0) min = d;
                }
                yield min == null ? CopDatum.nil() : min;
            }

            // --- Date/time functions ---
            case "NOW", "CURRENT_TIMESTAMP" -> CopDatum.of(LocalDateTime.now());
            case "CURDATE", "CURRENT_DATE" -> CopDatum.of(LocalDate.now().atStartOfDay());
            case "CURTIME", "CURRENT_TIME" -> CopDatum.of(LocalTime.now().toString());
            case "YEAR" -> nullSafe1(args, d -> CopDatum.of((long) toDateTime(d).getYear()));
            case "MONTH" -> nullSafe1(args, d -> CopDatum.of((long) toDateTime(d).getMonthValue()));
            case "DAY", "DAYOFMONTH" -> nullSafe1(args, d -> CopDatum.of((long) toDateTime(d).getDayOfMonth()));
            case "HOUR" -> nullSafe1(args, d -> CopDatum.of((long) toDateTime(d).getHour()));
            case "MINUTE" -> nullSafe1(args, d -> CopDatum.of((long) toDateTime(d).getMinute()));
            case "SECOND" -> nullSafe1(args, d -> CopDatum.of((long) toDateTime(d).getSecond()));
            case "DAYOFWEEK" -> nullSafe1(args, d -> CopDatum.of((long) (toDateTime(d).getDayOfWeek().getValue() % 7) + 1));
            case "DAYOFYEAR" -> nullSafe1(args, d -> CopDatum.of((long) toDateTime(d).getDayOfYear()));
            case "DATEDIFF" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                LocalDateTime a = toDateTime(args.get(0));
                LocalDateTime b = toDateTime(args.get(1));
                yield CopDatum.of(ChronoUnit.DAYS.between(b.toLocalDate(), a.toLocalDate()));
            }
            case "DATE_ADD", "ADDDATE" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                LocalDateTime dt = toDateTime(args.get(0));
                long days = args.get(1).toLong();
                yield CopDatum.of(dt.plusDays(days));
            }
            case "DATE_SUB", "SUBDATE" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                LocalDateTime dt = toDateTime(args.get(0));
                long days = args.get(1).toLong();
                yield CopDatum.of(dt.minusDays(days));
            }
            case "UNIX_TIMESTAMP" -> {
                if (args.isEmpty()) yield CopDatum.of(System.currentTimeMillis() / 1000L);
                if (args.get(0).isNull()) yield CopDatum.nil();
                LocalDateTime dt = toDateTime(args.get(0));
                yield CopDatum.of(dt.atZone(java.time.ZoneId.systemDefault()).toEpochSecond());
            }
            case "FROM_UNIXTIME" -> nullSafe1(args, d -> CopDatum.of(
                    LocalDateTime.ofInstant(java.time.Instant.ofEpochSecond(d.toLong()),
                            java.time.ZoneId.systemDefault())));
            case "DATE_FORMAT" -> {
                if (args.size() < 2 || args.get(0).isNull() || args.get(1).isNull()) yield CopDatum.nil();
                LocalDateTime dt = toDateTime(args.get(0));
                String fmt = mysqlToJavaDateFormat(args.get(1).toStringValue());
                yield CopDatum.of(dt.format(DateTimeFormatter.ofPattern(fmt)));
            }

            // --- Info functions ---
            case "VERSION" -> CopDatum.of("8.0.30-x-db");
            case "DATABASE", "SCHEMA" -> CopDatum.nil();
            case "USER", "CURRENT_USER" -> CopDatum.of("root@localhost");
            case "CONNECTION_ID" -> CopDatum.of(0L);

            default -> throw new UnsupportedOperationException("Unknown function: " + name);
        };
    }

    private static LocalDateTime toDateTime(CopDatum d) {
        if (d instanceof CopDatum.DateTimeVal dt) return dt.value();
        try {
            return LocalDateTime.parse(d.toStringValue().trim(),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        } catch (DateTimeParseException e) {
            try {
                return LocalDate.parse(d.toStringValue().trim()).atStartOfDay();
            } catch (DateTimeParseException e2) {
                return LocalDateTime.of(2000, 1, 1, 0, 0);
            }
        }
    }

    private static String mysqlToJavaDateFormat(String mysql) {
        return mysql.replace("%Y", "yyyy").replace("%y", "yy")
                .replace("%m", "MM").replace("%d", "dd")
                .replace("%H", "HH").replace("%i", "mm")
                .replace("%s", "ss").replace("%S", "ss")
                .replace("%M", "MMMM").replace("%b", "MMM")
                .replace("%W", "EEEE").replace("%a", "EEE")
                .replace("%j", "DDD").replace("%T", "HH:mm:ss")
                .replace("%r", "hh:mm:ss a");
    }

    @FunctionalInterface
    private interface DatumMapper { CopDatum apply(CopDatum d); }

    private static CopDatum nullSafe1(List<CopDatum> args, DatumMapper fn) {
        if (args.isEmpty() || args.get(0).isNull()) return CopDatum.nil();
        return fn.apply(args.get(0));
    }
}
