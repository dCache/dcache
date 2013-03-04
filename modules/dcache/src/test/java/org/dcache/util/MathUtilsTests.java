package org.dcache.util;

import org.junit.Test;

import static org.dcache.util.MathUtils.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
        assertThat(addWithInfinity(0L, 0L), is(0L));
        assertThat(addWithInfinity(0L, Long.MAX_VALUE), is(Long.MAX_VALUE));
        assertThat(addWithInfinity(5L, Long.MAX_VALUE), is(Long.MAX_VALUE));
        assertThat(addWithInfinity(-5L, Long.MAX_VALUE), is(Long.MAX_VALUE));
        assertThat(addWithInfinity(Long.MAX_VALUE, 0), is(Long.MAX_VALUE));
        assertThat(addWithInfinity(Long.MAX_VALUE, 5), is(Long.MAX_VALUE));
        assertThat(addWithInfinity(Long.MAX_VALUE, -5), is(Long.MAX_VALUE));
        assertThat(addWithInfinity(Long.MAX_VALUE, Long.MAX_VALUE), is(Long.MAX_VALUE));

        assertThat(addWithInfinity(0L, Long.MIN_VALUE), is(Long.MIN_VALUE));
        assertThat(addWithInfinity(5L, Long.MIN_VALUE), is(Long.MIN_VALUE));
        assertThat(addWithInfinity(-5L, Long.MIN_VALUE), is(Long.MIN_VALUE));
        assertThat(addWithInfinity(Long.MIN_VALUE, 0), is(Long.MIN_VALUE));
        assertThat(addWithInfinity(Long.MIN_VALUE, 5), is(Long.MIN_VALUE));
        assertThat(addWithInfinity(Long.MIN_VALUE, -5), is(Long.MIN_VALUE));
        assertThat(addWithInfinity(Long.MIN_VALUE, Long.MIN_VALUE), is(Long.MIN_VALUE));

        assertThat(addWithInfinity(10L, 20L), is(30L));
    }

    @Test
    public void testAddWithInfinityOverflow()
    {
        assertThat(addWithInfinity(Long.MAX_VALUE / 2 + 100, Long.MAX_VALUE / 2 + 100), is(Long.MAX_VALUE));
    }

    @Test
    public void testAddWithInfinityUnderflow()
    {
        assertThat(addWithInfinity(-Long.MAX_VALUE / 2 - 100, -Long.MAX_VALUE / 2 - 100), is(Long.MIN_VALUE));
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
        assertThat(subWithInfinity(0L, 0L), is(0L));
        assertThat(subWithInfinity(0L, Long.MAX_VALUE), is(Long.MIN_VALUE));
        assertThat(subWithInfinity(5L, Long.MAX_VALUE), is(Long.MIN_VALUE));
        assertThat(subWithInfinity(-5L, Long.MAX_VALUE), is(Long.MIN_VALUE));
        assertThat(subWithInfinity(Long.MAX_VALUE, 0), is(Long.MAX_VALUE));
        assertThat(subWithInfinity(Long.MAX_VALUE, 5), is(Long.MAX_VALUE));
        assertThat(subWithInfinity(Long.MAX_VALUE, -5), is(Long.MAX_VALUE));
        assertThat(subWithInfinity(Long.MAX_VALUE, Long.MIN_VALUE), is(Long.MAX_VALUE));

        assertThat(subWithInfinity(0L, Long.MIN_VALUE), is(Long.MAX_VALUE));
        assertThat(subWithInfinity(5L, Long.MIN_VALUE), is(Long.MAX_VALUE));
        assertThat(subWithInfinity(-5L, Long.MIN_VALUE), is(Long.MAX_VALUE));
        assertThat(subWithInfinity(Long.MIN_VALUE, 0), is(Long.MIN_VALUE));
        assertThat(subWithInfinity(Long.MIN_VALUE, 5), is(Long.MIN_VALUE));
        assertThat(subWithInfinity(Long.MIN_VALUE, -5), is(Long.MIN_VALUE));
        assertThat(subWithInfinity(Long.MIN_VALUE, Long.MAX_VALUE), is(Long.MIN_VALUE));

        assertThat(subWithInfinity(10L, 20L), is(-10L));
    }

    @Test
    public void testSubWithInfinityUnderflow()
    {
        assertThat(subWithInfinity(-Long.MAX_VALUE / 2 - 100, Long.MAX_VALUE / 2 + 100), is(Long.MIN_VALUE));
    }

    @Test
    public void testSubWithInfinityOverlow()
    {
        assertThat(subWithInfinity(Long.MAX_VALUE / 2 + 100, -Long.MAX_VALUE / 2 - 100), is(Long.MAX_VALUE));
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
