package org.dcache.util;

public class MathUtils
{
    /**
     * Return the absolute value of an integer, modulo some other integer.
     * This is similar to the naive {@code Math.abs(value) % modulo} except it handles
     * the case when value is Integer.MIN_VALUE correctly.
     */
    public static int absModulo(int value, int modulo)
    {
        return Math.abs(value % modulo);
    }

    /**
     * Implements long addition, treating MAX_VALUE and MIN_VALUE as positive
     * and negative infinity respectively. Semantics are similar to Double
     * arithmetic. Overflow results in positive infinity, underflow in
     * negative infinity.
     *
     * There is no NaN, thus addition of negative and positive throws
     * ArithmeticException instead.
     *
     * Note that MAX_VALUE != -MIN_VALUE, which is a derivation from Double
     * arithmetic.
     *
     * @throws ArithmeticException when adding MIN_VALUE and MAX_VALUE
     */
    public static long addWithInfinity(long a, long b)
    {
        if (a == Long.MIN_VALUE && b == Long.MAX_VALUE ||
                a == Long.MAX_VALUE && b == Long.MIN_VALUE) {
            throw new ArithmeticException("NaN");
        }
        if (a == Long.MAX_VALUE || b == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        if (a == Long.MIN_VALUE || b == Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        }

        long result = a + b;
        if ((a ^ b) < 0 | (a ^ result) >= 0) {
            return result;
        }
        return (a < 0) ? Long.MIN_VALUE : Long.MAX_VALUE;
    }

    /**
     * Implements long subtraction, treating MAX_VALUE and MIN_VALUE as
     * positive and negative infinity respectively. Semantics are similar
     * to Double arithmetic. Overflow results in positive infinity, underflow
     * in negative infinity.
     *
     * There is no NaN, thus addition of negative and positive throws
     * ArithmeticException instead.
     *
     * Note that MAX_VALUE != -MIN_VALUE, which is a derivation from Double
     * arithmetic.
     *
     * @throws ArithmeticException when subtracting MIN_VALUE from MIN_VALUE
     * or MAX_VALUE from MAX_VALUE
     */
    public static long subWithInfinity(long a, long b)
    {
        if (a == Long.MAX_VALUE && b == Long.MAX_VALUE ||
                a == Long.MIN_VALUE && b == Long.MIN_VALUE) {
            throw new ArithmeticException("NaN");
        }
        if (a == Long.MIN_VALUE || b == Long.MAX_VALUE) {
            return Long.MIN_VALUE;
        }
        if (a == Long.MAX_VALUE || b == Long.MIN_VALUE) {
            return Long.MAX_VALUE;
        }

        long result = a - b;
        if ((a ^ b) >= 0 | (a ^ result) >= 0) {
            return result;
        }

        return (a < 0) ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
}
