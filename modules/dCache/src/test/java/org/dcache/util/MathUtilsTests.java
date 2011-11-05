package org.dcache.util;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class MathUtilsTests {

    @Test
    public void testAbsModulo5() {
        assertEquals( 1, MathUtils.absModulo( -6, 5));
        assertEquals( 0, MathUtils.absModulo( -5, 5));
        assertEquals( 4, MathUtils.absModulo( -4, 5));
        assertEquals( 3, MathUtils.absModulo( -3, 5));
        assertEquals( 2, MathUtils.absModulo( -2, 5));
        assertEquals( 1, MathUtils.absModulo( -1, 5));
        assertEquals( 0, MathUtils.absModulo( 0, 5));
        assertEquals( 1, MathUtils.absModulo( 1, 5));
        assertEquals( 2, MathUtils.absModulo( 2, 5));
        assertEquals( 3, MathUtils.absModulo( 3, 5));
        assertEquals( 4, MathUtils.absModulo( 4, 5));
        assertEquals( 0, MathUtils.absModulo( 5, 5));
        assertEquals( 1, MathUtils.absModulo( 6, 5));
    }

    @Test
    public void testAbsModulo3() {
        assertEquals( 0, MathUtils.absModulo( -6, 3));
        assertEquals( 2, MathUtils.absModulo( -5, 3));
        assertEquals( 1, MathUtils.absModulo( -4, 3));
        assertEquals( 0, MathUtils.absModulo( -3, 3));
        assertEquals( 2, MathUtils.absModulo( -2, 3));
        assertEquals( 1, MathUtils.absModulo( -1, 3));
        assertEquals( 0, MathUtils.absModulo( 0, 3));
        assertEquals( 1, MathUtils.absModulo( 1, 3));
        assertEquals( 2, MathUtils.absModulo( 2, 3));
        assertEquals( 0, MathUtils.absModulo( 3, 3));
        assertEquals( 1, MathUtils.absModulo( 4, 3));
        assertEquals( 2, MathUtils.absModulo( 5, 3));
        assertEquals( 0, MathUtils.absModulo( 6, 3));
    }


    @Test
    public void testMathAbsModuloSmallestNumber() {
        int expected = (int)(Math.abs((long)Integer.MIN_VALUE) % 11);
        assertEquals(expected, MathUtils.absModulo(Integer.MIN_VALUE, 11));

        expected = (int)(Math.abs((long)Integer.MIN_VALUE) % 19);
        assertEquals(expected, MathUtils.absModulo(Integer.MIN_VALUE, 19));

        expected = (int)(Math.abs((long)Integer.MIN_VALUE) % 31);
        assertEquals(expected, MathUtils.absModulo(Integer.MIN_VALUE, 31));
    }


    @Test
    public void testMathAbsModuloLargestNumber() {
        int expected = (int)(Math.abs((long)Integer.MAX_VALUE) % 11);
        assertEquals(expected, MathUtils.absModulo(Integer.MAX_VALUE, 11));

        expected = (int)(Math.abs((long)Integer.MAX_VALUE) % 19);
        assertEquals(expected, MathUtils.absModulo(Integer.MAX_VALUE, 19));

        expected = (int)(Math.abs((long)Integer.MAX_VALUE) % 31);
        assertEquals(expected, MathUtils.absModulo(Integer.MAX_VALUE, 31));
    }

    @Test
    public void testMathAbsModuloMaxMax() {
        assertEquals(0, MathUtils.absModulo(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    @Test
    public void testMathAbsModuloMinMax() {
        assertEquals(1, MathUtils.absModulo(Integer.MIN_VALUE, Integer.MAX_VALUE));
    }
}
