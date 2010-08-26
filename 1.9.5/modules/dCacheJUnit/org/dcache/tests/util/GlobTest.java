package org.dcache.tests.util;

import org.junit.Test;
import static org.junit.Assert.*;

import org.dcache.util.Glob;

public class GlobTest
{
    @Test
    public void testNonWildcardMatch()
    {
        Glob pattern = new Glob("foobar");

        assertTrue(pattern.matches("foobar"));
        assertFalse(pattern.matches(""));
        assertFalse(pattern.matches("foobar1"));
        assertFalse(pattern.matches("foo*bar"));
    }

    @Test
    public void testSingleCharacterWildcard()
    {
        Glob pattern = new Glob("foo?bar");

        assertTrue(pattern.matches("foo-bar"));
        assertTrue(pattern.matches("foo?bar"));
        assertTrue(pattern.matches("foo*bar"));
        assertFalse(pattern.matches(""));
        assertFalse(pattern.matches("foo1bar1"));
        assertFalse(pattern.matches("foobar"));
    }

    @Test
    public void testMultiCharacterWildcard()
    {
        Glob pattern = new Glob("foo*bar");

        assertTrue(pattern.matches("foo-bar"));
        assertTrue(pattern.matches("foo?bar"));
        assertTrue(pattern.matches("foo*bar"));
        assertTrue(pattern.matches("foofoobar"));
        assertTrue(pattern.matches("foobar"));
        assertFalse(pattern.matches(""));
        assertFalse(pattern.matches("foo1bar1"));
    }

    @Test
    public void testMultiWildcard1()
    {
        Glob pattern = new Glob("foo**bar");

        assertTrue(pattern.matches("foo-bar"));
        assertTrue(pattern.matches("foo?bar"));
        assertTrue(pattern.matches("foo*bar"));
        assertTrue(pattern.matches("foofoobar"));
        assertTrue(pattern.matches("foobar"));
        assertFalse(pattern.matches(""));
        assertFalse(pattern.matches("foo1bar1"));
    }

    @Test
    public void testMultiWildcard2()
    {
        Glob pattern = new Glob("foo*?bar");

        assertTrue(pattern.matches("foo-bar"));
        assertTrue(pattern.matches("foo?bar"));
        assertTrue(pattern.matches("foo*bar"));
        assertTrue(pattern.matches("foofoobar"));
        assertFalse(pattern.matches("foobar"));
        assertFalse(pattern.matches(""));
        assertFalse(pattern.matches("foo1bar1"));
    }

    @Test
    public void testMultiWildcard3()
    {
        Glob pattern = new Glob("*foo*bar");

        assertTrue(pattern.matches("foo-bar"));
        assertTrue(pattern.matches("foo?bar"));
        assertTrue(pattern.matches("foo*bar"));
        assertTrue(pattern.matches("foofoobar"));
        assertTrue(pattern.matches("foobar"));
        assertFalse(pattern.matches(""));
        assertFalse(pattern.matches("foo1bar1"));
    }

    @Test
    public void testMultiWildcard4()
    {
        Glob pattern = new Glob("foo*bar*");

        assertTrue(pattern.matches("foo-bar"));
        assertTrue(pattern.matches("foo?bar"));
        assertTrue(pattern.matches("foo*bar"));
        assertTrue(pattern.matches("foofoobar"));
        assertTrue(pattern.matches("foobar"));
        assertTrue(pattern.matches("foo1bar1"));
        assertFalse(pattern.matches(""));
    }

    @Test
    public void testOnlyWildcard1()
    {
        Glob pattern = new Glob("*");

        assertTrue(pattern.matches("foo-bar"));
        assertTrue(pattern.matches("foo?bar"));
        assertTrue(pattern.matches("foo*bar"));
        assertTrue(pattern.matches("foofoobar"));
        assertTrue(pattern.matches("foobar"));
        assertTrue(pattern.matches("foo1bar1"));
        assertTrue(pattern.matches(""));
    }

    @Test
    public void testOnlyWildcard2()
    {
        Glob pattern = new Glob("?");

        assertFalse(pattern.matches("foo-bar"));
        assertFalse(pattern.matches("foo?bar"));
        assertFalse(pattern.matches("foo*bar"));
        assertFalse(pattern.matches("foofoobar"));
        assertFalse(pattern.matches("foobar"));
        assertFalse(pattern.matches("foo1bar1"));
        assertFalse(pattern.matches(""));
        assertTrue(pattern.matches("a"));
        assertTrue(pattern.matches("?"));
        assertTrue(pattern.matches("*"));
    }

    @Test
    public void testPatternEscape()
    {
        Glob pattern = new Glob("\\Q?");

        assertFalse(pattern.matches("\\Q"));
        assertTrue(pattern.matches("\\Qa"));
        assertTrue(pattern.matches("\\Q\\"));
    }
}