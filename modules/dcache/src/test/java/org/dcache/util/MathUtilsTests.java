package org.dcache.util;

import static org.dcache.util.MathUtils.*;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MathUtilsTests {

    @Test
    public void testAbsModulo5() {
        assertEquals( 1, absModulo(-6, 5));
        assertEquals( 0, absModulo(-5, 5));
        assertEquals( 4, absModulo(-4, 5));
        assertEquals( 3, absModulo(-3, 5));
        assertEquals( 2, absModulo(-2, 5));
        assertEquals( 1, absModulo(-1, 5));
        assertEquals( 0, absModulo(0, 5));
        assertEquals( 1, absModulo(1, 5));
        assertEquals( 2, absModulo(2, 5));
        assertEquals( 3, absModulo(3, 5));
        assertEquals( 4, absModulo(4, 5));
        assertEquals( 0, absModulo(5, 5));
        assertEquals( 1, absModulo(6, 5));
    }

    @Test
    public void testAbsModulo3() {
        assertEquals( 0, absModulo(-6, 3));
        assertEquals( 2, absModulo(-5, 3));
        assertEquals( 1, absModulo(-4, 3));
        assertEquals( 0, absModulo(-3, 3));
        assertEquals( 2, absModulo(-2, 3));
        assertEquals( 1, absModulo(-1, 3));
        assertEquals( 0, absModulo(0, 3));
        assertEquals( 1, absModulo(1, 3));
        assertEquals( 2, absModulo(2, 3));
        assertEquals( 0, absModulo(3, 3));
        assertEquals( 1, absModulo(4, 3));
        assertEquals( 2, absModulo(5, 3));
        assertEquals( 0, absModulo(6, 3));
    }


    @Test
    public void testMathAbsModuloSmallestNumber() {
        int expected = (int)(Math.abs((long)Integer.MIN_VALUE) % 11);
        assertEquals(expected, absModulo(Integer.MIN_VALUE, 11));

        expected = (int)(Math.abs((long)Integer.MIN_VALUE) % 19);
        assertEquals(expected, absModulo(Integer.MIN_VALUE, 19));

        expected = (int)(Math.abs((long)Integer.MIN_VALUE) % 31);
        assertEquals(expected, absModulo(Integer.MIN_VALUE, 31));
    }


    @Test
    public void testMathAbsModuloLargestNumber() {
        int expected = (int)(Math.abs((long)Integer.MAX_VALUE) % 11);
        assertEquals(expected, absModulo(Integer.MAX_VALUE, 11));

        expected = (int)(Math.abs((long)Integer.MAX_VALUE) % 19);
        assertEquals(expected, absModulo(Integer.MAX_VALUE, 19));

        expected = (int)(Math.abs((long)Integer.MAX_VALUE) % 31);
        assertEquals(expected, absModulo(Integer.MAX_VALUE, 31));
    }

    @Test
    public void testMathAbsModuloMaxMax() {
        assertEquals(0, absModulo(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    @Test
    public void testMathAbsModuloMinMax() {
        assertEquals(1, absModulo(Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    @Test
    public void testAddWithInfinity()
    {
        assertEquals(0L, addWithInfinity(0L, 0L));
        assertEquals(Long.MAX_VALUE, addWithInfinity(0L, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, addWithInfinity(5L, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, addWithInfinity(-5L, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, addWithInfinity(Long.MAX_VALUE, 0));
        assertEquals(Long.MAX_VALUE, addWithInfinity(Long.MAX_VALUE, 5));
        assertEquals(Long.MAX_VALUE, addWithInfinity(Long.MAX_VALUE, -5));
        assertEquals(Long.MAX_VALUE, addWithInfinity(Long.MAX_VALUE, Long.MAX_VALUE));

        assertEquals(Long.MIN_VALUE, addWithInfinity(0L, Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, addWithInfinity(5L, Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, addWithInfinity(-5L, Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, addWithInfinity(Long.MIN_VALUE, 0));
        assertEquals(Long.MIN_VALUE, addWithInfinity(Long.MIN_VALUE, 5));
        assertEquals(Long.MIN_VALUE, addWithInfinity(Long.MIN_VALUE, -5));
        assertEquals(Long.MIN_VALUE, addWithInfinity(Long.MIN_VALUE, Long.MIN_VALUE));

        assertEquals(30, addWithInfinity(10L, 20L));
    }

    @Test
    public void testAddWithInfinityOverflow()
    {
        assertEquals(Long.MAX_VALUE, addWithInfinity(Long.MAX_VALUE / 2 + 100, Long.MAX_VALUE / 2 + 100));
    }

    @Test
    public void testAddWithInfinityUnderflow()
    {
        assertEquals(Long.MIN_VALUE, addWithInfinity(-Long.MAX_VALUE / 2 - 100, -Long.MAX_VALUE / 2 - 100));
    }

    @Test(expected=ArithmeticException.class)
    public void testAddWithInfinityMaxMin()
    {
        addWithInfinity(Long.MAX_VALUE, Long.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void testAddWithInfinityMinMax()
    {
        addWithInfinity(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void testSubWithInfinity()
    {
        assertEquals(0L, subWithInfinity(0L, 0L));
        assertEquals(Long.MIN_VALUE, subWithInfinity(0L, Long.MAX_VALUE));
        assertEquals(Long.MIN_VALUE, subWithInfinity(5L, Long.MAX_VALUE));
        assertEquals(Long.MIN_VALUE, subWithInfinity(-5L, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, subWithInfinity(Long.MAX_VALUE, 0));
        assertEquals(Long.MAX_VALUE, subWithInfinity(Long.MAX_VALUE, 5));
        assertEquals(Long.MAX_VALUE, subWithInfinity(Long.MAX_VALUE, -5));
        assertEquals(Long.MAX_VALUE, subWithInfinity(Long.MAX_VALUE, Long.MIN_VALUE));

        assertEquals(Long.MAX_VALUE, subWithInfinity(0L, Long.MIN_VALUE));
        assertEquals(Long.MAX_VALUE, subWithInfinity(5L, Long.MIN_VALUE));
        assertEquals(Long.MAX_VALUE, subWithInfinity(-5L, Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, subWithInfinity(Long.MIN_VALUE, 0));
        assertEquals(Long.MIN_VALUE, subWithInfinity(Long.MIN_VALUE, 5));
        assertEquals(Long.MIN_VALUE, subWithInfinity(Long.MIN_VALUE, -5));
        assertEquals(Long.MIN_VALUE, subWithInfinity(Long.MIN_VALUE, Long.MAX_VALUE));

        assertEquals(-10L, subWithInfinity(10L, 20L));
    }

    @Test
    public void testSubWithInfinityUnderflow()
    {
        assertEquals(Long.MIN_VALUE, subWithInfinity(-Long.MAX_VALUE / 2 - 100, Long.MAX_VALUE / 2 + 100));
    }

    @Test
    public void testSubWithInfinityOverlow()
    {
        assertEquals(Long.MAX_VALUE, subWithInfinity(Long.MAX_VALUE / 2 + 100, -Long.MAX_VALUE / 2 - 100));
    }

    @Test(expected=ArithmeticException.class)
    public void testSubWithInfinityMaxMax()
    {
        subWithInfinity(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void testSubWithInfinityMinMin()
    {
        subWithInfinity(Long.MIN_VALUE, Long.MIN_VALUE);
    }

}
