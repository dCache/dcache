package org.dcache.util;

public class MathUtils {

    /**
     * Return the absolute value of an integer, modulo some other integer.
     * This is similar to the naive {@code Math.abs(value) % modulo} except it handles
     * the case when value is Integer.MIN_VALUE correctly.
     */
    static public int absModulo(int value, int modulo)
    {
        return Math.abs(value % modulo);
    }
}
