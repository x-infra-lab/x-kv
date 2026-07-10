package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import io.github.xinfra.lab.xkv.proto.Tipb;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExprEvaluatorTest {

    private static final int DT_BIGINT = 3;
    private static final int DT_DOUBLE = 5;
    private static final int DT_VARCHAR = 8;

    private static CopRow row(CopDatum... values) {
        return new CopRow(values);
    }

    private static Tipb.Expr intConst(long v) {
        return Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.CONSTANT)
                .setVal(Tipb.Datum.newBuilder().setIntVal(v))
                .build();
    }

    private static Tipb.Expr doubleConst(double v) {
        return Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.CONSTANT)
                .setVal(Tipb.Datum.newBuilder().setDoubleVal(v))
                .build();
    }

    private static Tipb.Expr strConst(String v) {
        return Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.CONSTANT)
                .setVal(Tipb.Datum.newBuilder().setStringVal(v))
                .build();
    }

    private static Tipb.Expr nullConst() {
        return Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.CONSTANT)
                .setVal(Tipb.Datum.newBuilder().setNullVal(true))
                .build();
    }

    private static Tipb.Expr colRef(int index) {
        return Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnIndex(index)
                .build();
    }

    private static Tipb.Expr binaryOp(int op, Tipb.Expr left, Tipb.Expr right) {
        return Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.BINARY_OP)
                .setOp(op)
                .addChildren(left)
                .addChildren(right)
                .build();
    }

    private static Tipb.Expr unaryOp(int op, Tipb.Expr child) {
        return Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.UNARY_OP)
                .setOp(op)
                .addChildren(child)
                .build();
    }

    private static Tipb.Expr func(String name, Tipb.Expr... args) {
        var b = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.FUNCTION_CALL)
                .setFuncName(name);
        for (var a : args) b.addChildren(a);
        return b.build();
    }

    private static CopDatum eval(Tipb.Expr expr) {
        return ExprEvaluator.eval(expr, row());
    }

    private static CopDatum eval(Tipb.Expr expr, CopRow row) {
        return ExprEvaluator.eval(expr, row);
    }

    // ============================================================
    // Constants and column refs
    // ============================================================

    @Test void constantInt() { assertThat(eval(intConst(42)).toLong()).isEqualTo(42); }
    @Test void constantDouble() { assertThat(eval(doubleConst(3.14)).toDouble()).isEqualTo(3.14); }
    @Test void constantString() { assertThat(eval(strConst("hello")).toStringValue()).isEqualTo("hello"); }
    @Test void constantNull() { assertThat(eval(nullConst()).isNull()).isTrue(); }

    @Test
    void constantDecimal() {
        var expr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.CONSTANT)
                .setVal(Tipb.Datum.newBuilder().setDecimalVal("123.456"))
                .build();
        assertThat(eval(expr).toStringValue()).isEqualTo("123.456");
    }

    @Test
    void columnRef() {
        CopRow r = row(CopDatum.of(10L), CopDatum.of("Alice"), CopDatum.of(30L));
        assertThat(eval(colRef(0), r).toLong()).isEqualTo(10);
        assertThat(eval(colRef(1), r).toStringValue()).isEqualTo("Alice");
        assertThat(eval(colRef(2), r).toLong()).isEqualTo(30);
    }

    // ============================================================
    // Arithmetic operations
    // ============================================================

    @Nested class ArithmeticTests {
        @Test void addInts() { assertThat(eval(binaryOp(0, intConst(3), intConst(5))).toLong()).isEqualTo(8); }
        @Test void subInts() { assertThat(eval(binaryOp(1, intConst(10), intConst(3))).toLong()).isEqualTo(7); }
        @Test void mulInts() { assertThat(eval(binaryOp(2, intConst(4), intConst(5))).toLong()).isEqualTo(20); }
        @Test void divDoubles() { assertThat(eval(binaryOp(3, intConst(10), intConst(3))).toDouble()).isCloseTo(3.333, org.assertj.core.data.Offset.offset(0.01)); }
        @Test void modInts() { assertThat(eval(binaryOp(4, intConst(10), intConst(3))).toLong()).isEqualTo(1); }
        @Test void intDivision() { assertThat(eval(binaryOp(5, intConst(10), intConst(3))).toLong()).isEqualTo(3); }

        @Test void addDoubleAndInt() { assertThat(eval(binaryOp(0, doubleConst(1.5), intConst(2))).toDouble()).isEqualTo(3.5); }

        @Test void divByZeroReturnsNull() { assertThat(eval(binaryOp(3, intConst(10), intConst(0))).isNull()).isTrue(); }
        @Test void modByZeroReturnsNull() { assertThat(eval(binaryOp(4, intConst(10), intConst(0))).isNull()).isTrue(); }
        @Test void intDivByZeroReturnsNull() { assertThat(eval(binaryOp(5, intConst(10), intConst(0))).isNull()).isTrue(); }

        @Test void addOverflowThrows() {
            assertThatThrownBy(() -> eval(binaryOp(0, intConst(Long.MAX_VALUE), intConst(1))))
                    .isInstanceOf(ArithmeticException.class);
        }

        @Test void mulOverflowThrows() {
            assertThatThrownBy(() -> eval(binaryOp(2, intConst(Long.MAX_VALUE), intConst(2))))
                    .isInstanceOf(ArithmeticException.class);
        }

        @Test void intDivOverflowThrows() {
            assertThatThrownBy(() -> eval(binaryOp(5, intConst(Long.MIN_VALUE), intConst(-1))))
                    .isInstanceOf(ArithmeticException.class);
        }

        @Test void nullLeftReturnsNull() { assertThat(eval(binaryOp(0, nullConst(), intConst(1))).isNull()).isTrue(); }
        @Test void nullRightReturnsNull() { assertThat(eval(binaryOp(0, intConst(1), nullConst())).isNull()).isTrue(); }
    }

    // ============================================================
    // Comparison operations
    // ============================================================

    @Nested class ComparisonTests {
        @Test void eq() { assertThat(eval(binaryOp(6, intConst(5), intConst(5))).toLong()).isEqualTo(1); }
        @Test void eqFalse() { assertThat(eval(binaryOp(6, intConst(5), intConst(3))).toLong()).isEqualTo(0); }
        @Test void ne() { assertThat(eval(binaryOp(7, intConst(5), intConst(3))).toLong()).isEqualTo(1); }
        @Test void lt() { assertThat(eval(binaryOp(8, intConst(3), intConst(5))).toLong()).isEqualTo(1); }
        @Test void ltFalse() { assertThat(eval(binaryOp(8, intConst(5), intConst(3))).toLong()).isEqualTo(0); }
        @Test void le() { assertThat(eval(binaryOp(9, intConst(5), intConst(5))).toLong()).isEqualTo(1); }
        @Test void gt() { assertThat(eval(binaryOp(10, intConst(5), intConst(3))).toLong()).isEqualTo(1); }
        @Test void ge() { assertThat(eval(binaryOp(11, intConst(5), intConst(5))).toLong()).isEqualTo(1); }

        @Test void compareStrings() { assertThat(eval(binaryOp(8, strConst("abc"), strConst("xyz"))).toLong()).isEqualTo(1); }
        @Test void compareNullReturnsNull() { assertThat(eval(binaryOp(6, nullConst(), intConst(5))).isNull()).isTrue(); }
    }

    // ============================================================
    // Logical operations (AND / OR)
    // ============================================================

    @Nested class LogicTests {
        @Test void andTrueTrue() { assertThat(eval(binaryOp(12, intConst(1), intConst(1))).toLong()).isEqualTo(1); }
        @Test void andTrueFalse() { assertThat(eval(binaryOp(12, intConst(1), intConst(0))).toLong()).isEqualTo(0); }
        @Test void andFalseShortCircuit() { assertThat(eval(binaryOp(12, intConst(0), intConst(1))).toLong()).isEqualTo(0); }
        @Test void andNullFalse() { assertThat(eval(binaryOp(12, nullConst(), intConst(0))).toLong()).isEqualTo(0); }
        @Test void andNullTrue() { assertThat(eval(binaryOp(12, nullConst(), intConst(1))).isNull()).isTrue(); }
        @Test void andTrueNull() { assertThat(eval(binaryOp(12, intConst(1), nullConst())).isNull()).isTrue(); }

        @Test void orTrueFalse() { assertThat(eval(binaryOp(13, intConst(1), intConst(0))).toLong()).isEqualTo(1); }
        @Test void orFalseFalse() { assertThat(eval(binaryOp(13, intConst(0), intConst(0))).toLong()).isEqualTo(0); }
        @Test void orTrueShortCircuit() { assertThat(eval(binaryOp(13, intConst(1), intConst(0))).toLong()).isEqualTo(1); }
        @Test void orNullTrue() { assertThat(eval(binaryOp(13, nullConst(), intConst(1))).toLong()).isEqualTo(1); }
        @Test void orNullFalse() { assertThat(eval(binaryOp(13, nullConst(), intConst(0))).isNull()).isTrue(); }
        @Test void orFalseNull() { assertThat(eval(binaryOp(13, intConst(0), nullConst())).isNull()).isTrue(); }
    }

    // ============================================================
    // Unary operations
    // ============================================================

    @Nested class UnaryTests {
        @Test void notTrue() { assertThat(eval(unaryOp(0, intConst(1))).toLong()).isEqualTo(0); }
        @Test void notFalse() { assertThat(eval(unaryOp(0, intConst(0))).toLong()).isEqualTo(1); }
        @Test void notNull() { assertThat(eval(unaryOp(0, nullConst())).isNull()).isTrue(); }

        @Test void negInt() { assertThat(eval(unaryOp(1, intConst(5))).toLong()).isEqualTo(-5); }
        @Test void negDouble() { assertThat(eval(unaryOp(1, doubleConst(3.14))).toDouble()).isEqualTo(-3.14); }
        @Test void negNull() { assertThat(eval(unaryOp(1, nullConst())).isNull()).isTrue(); }
        @Test void negMinValuePromotesToDouble() {
            assertThat(eval(unaryOp(1, intConst(Long.MIN_VALUE))).toDouble()).isGreaterThan(0);
        }

        @Test void isNull() { assertThat(eval(unaryOp(2, nullConst())).toLong()).isEqualTo(1); }
        @Test void isNullFalse() { assertThat(eval(unaryOp(2, intConst(5))).toLong()).isEqualTo(0); }
        @Test void isNotNull() { assertThat(eval(unaryOp(3, intConst(5))).toLong()).isEqualTo(1); }
        @Test void isNotNullFalse() { assertThat(eval(unaryOp(3, nullConst())).toLong()).isEqualTo(0); }
    }

    // ============================================================
    // LIKE
    // ============================================================

    @Nested class LikeTests {
        private Tipb.Expr like(Tipb.Expr value, Tipb.Expr pattern, boolean not) {
            return Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.LIKE)
                    .setNot(not)
                    .addChildren(value)
                    .addChildren(pattern)
                    .build();
        }

        @Test void exactMatch() { assertThat(eval(like(strConst("hello"), strConst("hello"), false)).toLong()).isEqualTo(1); }
        @Test void percentWildcard() { assertThat(eval(like(strConst("hello world"), strConst("hello%"), false)).toLong()).isEqualTo(1); }
        @Test void underscoreWildcard() { assertThat(eval(like(strConst("abc"), strConst("a_c"), false)).toLong()).isEqualTo(1); }
        @Test void noMatch() { assertThat(eval(like(strConst("hello"), strConst("world"), false)).toLong()).isEqualTo(0); }
        @Test void notLike() { assertThat(eval(like(strConst("hello"), strConst("world"), true)).toLong()).isEqualTo(1); }
        @Test void nullValue() { assertThat(eval(like(nullConst(), strConst("%"), false)).isNull()).isTrue(); }
        @Test void nullPattern() { assertThat(eval(like(strConst("hello"), nullConst(), false)).isNull()).isTrue(); }
        @Test void percentMiddle() { assertThat(eval(like(strConst("abcdef"), strConst("a%f"), false)).toLong()).isEqualTo(1); }
        @Test void caseInsensitive() { assertThat(eval(like(strConst("Hello"), strConst("hello"), false)).toLong()).isEqualTo(1); }
        @Test void escapedPercent() { assertThat(eval(like(strConst("100%"), strConst("100\\%"), false)).toLong()).isEqualTo(1); }
    }

    // ============================================================
    // IN
    // ============================================================

    @Nested class InTests {
        private Tipb.Expr inExpr(boolean not, Tipb.Expr value, Tipb.Expr... items) {
            var b = Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.IN)
                    .setNot(not)
                    .addChildren(value);
            for (var item : items) b.addChildren(item);
            return b.build();
        }

        @Test void inFound() { assertThat(eval(inExpr(false, intConst(3), intConst(1), intConst(2), intConst(3))).toLong()).isEqualTo(1); }
        @Test void inNotFound() { assertThat(eval(inExpr(false, intConst(4), intConst(1), intConst(2), intConst(3))).toLong()).isEqualTo(0); }
        @Test void notIn() { assertThat(eval(inExpr(true, intConst(4), intConst(1), intConst(2))).toLong()).isEqualTo(1); }
        @Test void inNullValue() { assertThat(eval(inExpr(false, nullConst(), intConst(1))).isNull()).isTrue(); }
        @Test void inWithNullItem() { assertThat(eval(inExpr(false, intConst(4), intConst(1), nullConst())).isNull()).isTrue(); }
        @Test void inFoundOverridesNull() { assertThat(eval(inExpr(false, intConst(1), intConst(1), nullConst())).toLong()).isEqualTo(1); }
    }

    // ============================================================
    // BETWEEN
    // ============================================================

    @Nested class BetweenTests {
        private Tipb.Expr between(boolean not, Tipb.Expr value, Tipb.Expr lo, Tipb.Expr hi) {
            return Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.BETWEEN)
                    .setNot(not)
                    .addChildren(value)
                    .addChildren(lo)
                    .addChildren(hi)
                    .build();
        }

        @Test void inRange() { assertThat(eval(between(false, intConst(5), intConst(1), intConst(10))).toLong()).isEqualTo(1); }
        @Test void atLowerBound() { assertThat(eval(between(false, intConst(1), intConst(1), intConst(10))).toLong()).isEqualTo(1); }
        @Test void atUpperBound() { assertThat(eval(between(false, intConst(10), intConst(1), intConst(10))).toLong()).isEqualTo(1); }
        @Test void outOfRange() { assertThat(eval(between(false, intConst(0), intConst(1), intConst(10))).toLong()).isEqualTo(0); }
        @Test void notBetween() { assertThat(eval(between(true, intConst(0), intConst(1), intConst(10))).toLong()).isEqualTo(1); }
        @Test void nullValue() { assertThat(eval(between(false, nullConst(), intConst(1), intConst(10))).isNull()).isTrue(); }
        @Test void nullBound() { assertThat(eval(between(false, intConst(5), nullConst(), intConst(10))).isNull()).isTrue(); }
    }

    // ============================================================
    // CAST
    // ============================================================

    @Nested class CastTests {
        private Tipb.Expr cast(Tipb.Expr value, int targetType) {
            return Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.CAST)
                    .setDataType(targetType)
                    .addChildren(value)
                    .build();
        }

        @Test void castToInt() { assertThat(eval(cast(doubleConst(3.7), DT_BIGINT)).toLong()).isEqualTo(3); }
        @Test void castToDouble() { assertThat(eval(cast(intConst(42), DT_DOUBLE)).toDouble()).isEqualTo(42.0); }
        @Test void castToString() { assertThat(eval(cast(intConst(42), DT_VARCHAR)).toStringValue()).isEqualTo("42"); }
        @Test void castToDecimal() { assertThat(eval(cast(intConst(42), 6)).toStringValue()).isEqualTo("42"); }
        @Test void castStringToDecimal() {
            var result = eval(cast(strConst("123.45"), 6));
            assertThat(result.toDouble()).isEqualTo(123.45);
        }
        @Test void castInvalidStringToDecimalReturnsNull() {
            assertThat(eval(cast(strConst("not_a_number"), 6)).isNull()).isTrue();
        }
        @Test void castNullReturnsNull() { assertThat(eval(cast(nullConst(), DT_BIGINT)).isNull()).isTrue(); }
        @Test void castStringToDatetime() {
            var result = eval(cast(strConst("2024-01-15 10:30:00"), 13));
            assertThat(result.isNull()).isFalse();
        }
        @Test void castInvalidDatetimeReturnsNull() {
            assertThat(eval(cast(strConst("not-a-date"), 13)).isNull()).isTrue();
        }
    }

    // ============================================================
    // CASE WHEN
    // ============================================================

    @Nested class CaseWhenTests {
        @Test void simpleSearchedCase() {
            // CASE WHEN true THEN 1 ELSE 0
            var expr = Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.CASE_WHEN)
                    .setOp(0)
                    .addChildren(intConst(1))  // condition (true)
                    .addChildren(intConst(42)) // result
                    .addChildren(intConst(0))  // else
                    .build();
            assertThat(eval(expr).toLong()).isEqualTo(42);
        }

        @Test void searchedCaseFallsToElse() {
            var expr = Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.CASE_WHEN)
                    .setOp(0)
                    .addChildren(intConst(0))  // condition (false)
                    .addChildren(intConst(42)) // result
                    .addChildren(intConst(99)) // else
                    .build();
            assertThat(eval(expr).toLong()).isEqualTo(99);
        }

        @Test void searchedCaseNoElseReturnsNull() {
            var expr = Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.CASE_WHEN)
                    .setOp(0)
                    .addChildren(intConst(0))  // condition (false)
                    .addChildren(intConst(42)) // result
                    .build();
            assertThat(eval(expr).isNull()).isTrue();
        }

        @Test void simpleCase() {
            // CASE col WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other'
            CopRow r = row(CopDatum.of(2L));
            var expr = Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.CASE_WHEN)
                    .setOp(1)
                    .addChildren(colRef(0))
                    .addChildren(intConst(1))
                    .addChildren(strConst("one"))
                    .addChildren(intConst(2))
                    .addChildren(strConst("two"))
                    .addChildren(strConst("other"))
                    .build();
            assertThat(eval(expr, r).toStringValue()).isEqualTo("two");
        }

        @Test void searchedCaseNullConditionSkipped() {
            var expr = Tipb.Expr.newBuilder()
                    .setTp(Tipb.ExprType.CASE_WHEN)
                    .setOp(0)
                    .addChildren(nullConst())  // null condition -> skip
                    .addChildren(intConst(42))
                    .addChildren(intConst(1))  // true condition
                    .addChildren(intConst(99))
                    .build();
            assertThat(eval(expr).toLong()).isEqualTo(99);
        }
    }

    // ============================================================
    // FUNCTION_CALL
    // ============================================================

    @Nested class FunctionTests {
        @Test void ifTrue() { assertThat(eval(func("IF", intConst(1), intConst(10), intConst(20))).toLong()).isEqualTo(10); }
        @Test void ifFalse() { assertThat(eval(func("IF", intConst(0), intConst(10), intConst(20))).toLong()).isEqualTo(20); }

        @Test void ifnullNotNull() { assertThat(eval(func("IFNULL", intConst(5), intConst(10))).toLong()).isEqualTo(5); }
        @Test void ifnullNull() { assertThat(eval(func("IFNULL", nullConst(), intConst(10))).toLong()).isEqualTo(10); }

        @Test void nullifEqual() { assertThat(eval(func("NULLIF", intConst(5), intConst(5))).isNull()).isTrue(); }
        @Test void nullifNotEqual() { assertThat(eval(func("NULLIF", intConst(5), intConst(3))).toLong()).isEqualTo(5); }

        @Test void coalesce() { assertThat(eval(func("COALESCE", nullConst(), nullConst(), intConst(42))).toLong()).isEqualTo(42); }
        @Test void coalesceAllNull() { assertThat(eval(func("COALESCE", nullConst(), nullConst())).isNull()).isTrue(); }

        @Test void concat() { assertThat(eval(func("CONCAT", strConst("hello"), strConst(" "), strConst("world"))).toStringValue()).isEqualTo("hello world"); }
        @Test void concatWithNull() { assertThat(eval(func("CONCAT", strConst("hello"), nullConst())).isNull()).isTrue(); }

        @Test void length() { assertThat(eval(func("LENGTH", strConst("hello"))).toLong()).isEqualTo(5); }
        @Test void lengthNull() { assertThat(eval(func("LENGTH", nullConst())).isNull()).isTrue(); }

        @Test void upper() { assertThat(eval(func("UPPER", strConst("hello"))).toStringValue()).isEqualTo("HELLO"); }
        @Test void lower() { assertThat(eval(func("LOWER", strConst("HELLO"))).toStringValue()).isEqualTo("hello"); }
        @Test void trim() { assertThat(eval(func("TRIM", strConst("  hi  "))).toStringValue()).isEqualTo("hi"); }

        @Test void abs() { assertThat(eval(func("ABS", intConst(-5))).toLong()).isEqualTo(5); }
        @Test void absMinValue() { assertThat(eval(func("ABS", intConst(Long.MIN_VALUE))).toDouble()).isGreaterThan(0); }
        @Test void absNull() { assertThat(eval(func("ABS", nullConst())).isNull()).isTrue(); }

        @Test void ceil() { assertThat(eval(func("CEIL", doubleConst(3.2))).toLong()).isEqualTo(4); }
        @Test void floor() { assertThat(eval(func("FLOOR", doubleConst(3.8))).toLong()).isEqualTo(3); }

        @Test void roundNoScale() { assertThat(eval(func("ROUND", doubleConst(3.5))).toLong()).isEqualTo(4); }
        @Test void roundWithScale() { assertThat(eval(func("ROUND", doubleConst(3.456), intConst(2))).toDouble()).isEqualTo(3.46); }
        @Test void roundNull() { assertThat(eval(func("ROUND", nullConst())).isNull()).isTrue(); }

        @Test void mod() { assertThat(eval(func("MOD", intConst(10), intConst(3))).toLong()).isEqualTo(1); }
        @Test void modByZero() { assertThat(eval(func("MOD", intConst(10), intConst(0))).isNull()).isTrue(); }
        @Test void modNull() { assertThat(eval(func("MOD", nullConst(), intConst(3))).isNull()).isTrue(); }

        @Test void now() { assertThat(eval(func("NOW")).isNull()).isFalse(); }
        @Test void version() { assertThat(eval(func("VERSION")).toStringValue()).contains("x-db"); }

        @Test void unknownFunctionThrows() {
            assertThatThrownBy(() -> eval(func("NONEXISTENT")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test void charLength() { assertThat(eval(func("CHAR_LENGTH", strConst("hello"))).toLong()).isEqualTo(5); }
        @Test void ucase() { assertThat(eval(func("UCASE", strConst("hello"))).toStringValue()).isEqualTo("HELLO"); }
        @Test void lcase() { assertThat(eval(func("LCASE", strConst("HELLO"))).toStringValue()).isEqualTo("hello"); }
        @Test void ceiling() { assertThat(eval(func("CEILING", doubleConst(3.2))).toLong()).isEqualTo(4); }
    }

    // ============================================================
    // passesFilter
    // ============================================================

    @Nested class FilterTests {
        @Test void emptyConditions() {
            assertThat(ExprEvaluator.passesFilter(row(CopDatum.of(1L)), List.of())).isTrue();
        }

        @Test void nullConditions() {
            assertThat(ExprEvaluator.passesFilter(row(CopDatum.of(1L)), null)).isTrue();
        }

        @Test void singleTrueCondition() {
            var cond = binaryOp(10, intConst(5), intConst(3)); // 5 > 3
            assertThat(ExprEvaluator.passesFilter(row(), List.of(cond))).isTrue();
        }

        @Test void singleFalseCondition() {
            var cond = binaryOp(10, intConst(1), intConst(3)); // 1 > 3
            assertThat(ExprEvaluator.passesFilter(row(), List.of(cond))).isFalse();
        }

        @Test void multipleConditionsAllTrue() {
            var c1 = binaryOp(10, intConst(5), intConst(3));
            var c2 = binaryOp(8, intConst(1), intConst(3));
            assertThat(ExprEvaluator.passesFilter(row(), List.of(c1, c2))).isTrue();
        }

        @Test void multipleConditionsOneFalse() {
            var c1 = binaryOp(10, intConst(5), intConst(3)); // true
            var c2 = binaryOp(10, intConst(1), intConst(3)); // false
            assertThat(ExprEvaluator.passesFilter(row(), List.of(c1, c2))).isFalse();
        }
    }

    // ============================================================
    // Proto round-trip
    // ============================================================

    @Nested class ProtoTests {
        @Test void intRoundTrip() {
            CopDatum orig = CopDatum.of(42L);
            Tipb.Datum proto = ExprEvaluator.toProto(orig);
            CopDatum back = ExprEvaluator.fromProto(proto);
            assertThat(back.toLong()).isEqualTo(42);
        }

        @Test void doubleRoundTrip() {
            CopDatum orig = CopDatum.of(3.14);
            Tipb.Datum proto = ExprEvaluator.toProto(orig);
            CopDatum back = ExprEvaluator.fromProto(proto);
            assertThat(back.toDouble()).isEqualTo(3.14);
        }

        @Test void stringRoundTrip() {
            CopDatum orig = CopDatum.of("hello");
            Tipb.Datum proto = ExprEvaluator.toProto(orig);
            CopDatum back = ExprEvaluator.fromProto(proto);
            assertThat(back.toStringValue()).isEqualTo("hello");
        }

        @Test void nullRoundTrip() {
            CopDatum orig = CopDatum.nil();
            Tipb.Datum proto = ExprEvaluator.toProto(orig);
            CopDatum back = ExprEvaluator.fromProto(proto);
            assertThat(back.isNull()).isTrue();
        }

        @Test void decimalRoundTrip() {
            CopDatum orig = CopDatum.of(new BigDecimal("123.456"));
            Tipb.Datum proto = ExprEvaluator.toProto(orig);
            CopDatum back = ExprEvaluator.fromProto(proto);
            assertThat(back.toStringValue()).isEqualTo("123.456");
        }
    }

    // ============================================================
    // likeMatch edge cases
    // ============================================================

    @Nested class LikeMatchTests {
        @Test void emptyStringEmptyPattern() { assertThat(ExprEvaluator.likeMatch("", "")).isTrue(); }
        @Test void emptyStringPercent() { assertThat(ExprEvaluator.likeMatch("", "%")).isTrue(); }
        @Test void emptyStringUnderscore() { assertThat(ExprEvaluator.likeMatch("", "_")).isFalse(); }
        @Test void multiplePercents() { assertThat(ExprEvaluator.likeMatch("abcdef", "%cd%")).isTrue(); }
        @Test void trailingPercent() { assertThat(ExprEvaluator.likeMatch("abc", "abc%")).isTrue(); }
        @Test void leadingPercent() { assertThat(ExprEvaluator.likeMatch("abc", "%abc")).isTrue(); }
        @Test void onlyPercent() { assertThat(ExprEvaluator.likeMatch("anything", "%")).isTrue(); }
        @Test void multipleUnderscores() { assertThat(ExprEvaluator.likeMatch("abc", "___")).isTrue(); }
        @Test void underscoreAndPercent() { assertThat(ExprEvaluator.likeMatch("abcdef", "_b%f")).isTrue(); }
    }

    // ============================================================
    // coerce edge cases
    // ============================================================

    @Nested class CoerceTests {
        @Test void coerceIntToTinyint() { assertThat(ExprEvaluator.coerce(CopDatum.of(42L), 0).toLong()).isEqualTo(42); }
        @Test void coerceIntToSmallint() { assertThat(ExprEvaluator.coerce(CopDatum.of(42L), 1).toLong()).isEqualTo(42); }
        @Test void coerceIntToFloat() { assertThat(ExprEvaluator.coerce(CopDatum.of(42L), 4).toDouble()).isEqualTo(42.0); }
        @Test void coerceDecimalPassthrough() {
            CopDatum d = CopDatum.of(new BigDecimal("3.14"));
            assertThat(ExprEvaluator.coerce(d, 6)).isSameAs(d);
        }
        @Test void coerceDoubleToDecimal() {
            var result = ExprEvaluator.coerce(CopDatum.of(3.14), 6);
            assertThat(result.toDouble()).isCloseTo(3.14, org.assertj.core.data.Offset.offset(0.001));
        }
        @Test void coerceToChar() { assertThat(ExprEvaluator.coerce(CopDatum.of(42L), 7).toStringValue()).isEqualTo("42"); }
        @Test void coerceToText() { assertThat(ExprEvaluator.coerce(CopDatum.of(42L), 9).toStringValue()).isEqualTo("42"); }
        @Test void coerceNull() { assertThat(ExprEvaluator.coerce(CopDatum.nil(), 3).isNull()).isTrue(); }
        @Test void coerceUnknownTypePassthrough() {
            CopDatum d = CopDatum.of(42L);
            assertThat(ExprEvaluator.coerce(d, 99)).isSameAs(d);
        }
    }
}
