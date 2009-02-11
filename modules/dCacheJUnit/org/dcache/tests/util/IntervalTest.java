package org.dcache.tests.util;

import static org.junit.Assert.*;
import org.junit.Test;

import org.dcache.util.Interval;

public class IntervalTest
{
    @Test
    public void closedInterval()
    {
        Interval i = Interval.parseInterval("3..10");
        assertFalse(i.contains(2));
        assertTrue(i.contains(3));
        assertTrue(i.contains(10));
        assertFalse(i.contains(11));
    }

    @Test
    public void openInterval1()
    {
        Interval i = Interval.parseInterval("..10");
        assertTrue(i.contains(Long.MIN_VALUE));
        assertTrue(i.contains(10));
        assertFalse(i.contains(11));
    }

    @Test
    public void openInterval2()
    {
        Interval i = Interval.parseInterval("3..");
        assertFalse(i.contains(2));
        assertTrue(i.contains(3));
        assertTrue(i.contains(Long.MAX_VALUE));
    }

    @Test
    public void openInterval3()
    {
        Interval i = Interval.parseInterval("..");
        assertTrue(i.contains(Long.MIN_VALUE));
        assertTrue(i.contains(Long.MAX_VALUE));
    }

    @Test
    public void emptyInterval()
    {
        Interval i = Interval.parseInterval("10..3");
        assertFalse(i.contains(2));
        assertFalse(i.contains(3));
        assertFalse(i.contains(10));
        assertFalse(i.contains(11));
    }

    @Test
    public void negativeBounds()
    {
        Interval i = Interval.parseInterval("-10..-3");
        assertFalse("Any value between -10 and -3", i.contains(-11));
        assertTrue("Any value between -10 and -3", i.contains(-10));
        assertTrue("Any value between -10 and -3", i.contains(-3));
        assertFalse("Any value between -10 and -3", i.contains(-2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidInterval1()
    {
        Interval.parseInterval("10..11.2");
    }


    @Test(expected=IllegalArgumentException.class)
    public void invalidInterval2()
    {
        Interval.parseInterval("10..11..12");
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidInterval3()
    {
        Interval.parseInterval("10,11,12");
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidInterval4()
    {
        Interval.parseInterval("a..b");
    }
}
