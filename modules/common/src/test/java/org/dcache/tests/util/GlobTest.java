package org.dcache.tests.util;

import org.junit.Test;

import java.util.regex.PatternSyntaxException;

import org.dcache.util.Glob;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

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

    @Test
    public void testCurlyBrackets()
    {
        Glob pattern = new Glob("{foo,bar}");

        assertFalse(pattern.matches("foo,bar"));
        assertTrue(pattern.matches("foo"));
        assertTrue(pattern.matches("bar"));
    }

    @Test
    public void testCurlyBracketsInsidePattern()
    {
        Glob pattern = new Glob("a{foo,bar}b");

        assertFalse(pattern.matches("ab"));
        assertTrue(pattern.matches("afoob"));
        assertTrue(pattern.matches("abarb"));
    }

    @Test
    public void testNestedCurlyBrackets()
    {
        Glob pattern = new Glob("{foo,{bar,baz*}}");

        assertFalse(pattern.matches("bar1"));
        assertTrue(pattern.matches("foo"));
        assertTrue(pattern.matches("bar"));
        assertTrue(pattern.matches("baz"));
        assertTrue(pattern.matches("baz1"));
    }

    @Test
    public void testIncompleteCurlyBrackets()
    {
        Glob pattern = new Glob("{foo{bar}");
        assertTrue(pattern.matches("{foobar"));
    }

    @Test
    public void testComma()
    {
        Glob pattern = new Glob(",");
        assertTrue(pattern.matches(","));
    }

    @Test
    public void testIsAnchored()
    {
        Glob pattern = new Glob("foo*bar");

        assertTrue(pattern.toPattern().matcher("foo-bar").find());
        assertFalse(pattern.toPattern().matcher("<foo-bar>").find());
    }

    @Test
    public void testGlobExpansion()
    {
        assertThat(Glob.expandGlob(""), containsInAnyOrder(""));
        assertThat(Glob.expandGlob("foo"), containsInAnyOrder("foo"));
        assertThat(Glob.expandGlob("foo{}"), containsInAnyOrder("foo"));
        assertThat(Glob.expandGlob("foo{a}"), containsInAnyOrder("fooa"));
        assertThat(Glob.expandGlob("foo{a,b}"), containsInAnyOrder("fooa", "foob"));
        assertThat(Glob.expandGlob("foo{a,b,c}"), containsInAnyOrder("fooa", "foob", "fooc"));
        assertThat(Glob.expandGlob("{}"), containsInAnyOrder(""));
        assertThat(Glob.expandGlob("{,}"), containsInAnyOrder("", ""));
        assertThat(Glob.expandGlob("{a,b,c}"), containsInAnyOrder("a", "b", "c"));
        assertThat(Glob.expandGlob("{a,b,c}foo"), containsInAnyOrder("afoo", "bfoo", "cfoo"));
        assertThat(Glob.expandGlob("foo{a,b,c}foo"), containsInAnyOrder("fooafoo", "foobfoo", "foocfoo"));
        assertThat(Glob.expandGlob("foo{a,bar{c,d}}"), containsInAnyOrder("fooa", "foobarc", "foobard"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGlobExpansionUnmatchedOpen()
    {
        Glob.expandGlob("{");
    }

    // REVISIT: This should not throw an exception
    @Test(expected = IllegalArgumentException.class)
    public void testGlobExpansionComma()
    {
        Glob.expandGlob(",");
    }

    // REVISIT: This should not throw an exception
    @Test(expected = IllegalArgumentException.class)
    public void testGlobExpansionComma2()
    {
        Glob.expandGlob("a,b");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGlobExpansionUnmatchedClose1()
    {
        Glob.expandGlob("}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGlobExpansionUnmatchedClose2()
    {
        Glob.expandGlob("{}}");
    }

    @Test
    public void testListExpansion()
    {
        assertThat(Glob.expandList(""), containsInAnyOrder(""));
        assertThat(Glob.expandList("foo"), containsInAnyOrder("foo"));
        assertThat(Glob.expandList("foo{}"), containsInAnyOrder("foo"));
        assertThat(Glob.expandList("foo{a}"), containsInAnyOrder("fooa"));
        assertThat(Glob.expandList("foo{a,b}"), containsInAnyOrder("fooa", "foob"));
        assertThat(Glob.expandList("foo{a,b,c}"), containsInAnyOrder("fooa", "foob", "fooc"));
        assertThat(Glob.expandList("{}"), containsInAnyOrder(""));
        assertThat(Glob.expandList("{,}"), containsInAnyOrder("", ""));
        assertThat(Glob.expandList(","), containsInAnyOrder("", ""));
        assertThat(Glob.expandList("{a,b,c}"), containsInAnyOrder("a", "b", "c"));
        assertThat(Glob.expandList("a,b,c"), containsInAnyOrder("a", "b", "c"));
        assertThat(Glob.expandList("{a,b,c}foo"), containsInAnyOrder("afoo", "bfoo", "cfoo"));
        assertThat(Glob.expandList("foo{a,b,c}foo"), containsInAnyOrder("fooafoo", "foobfoo", "foocfoo"));
        assertThat(Glob.expandList("foo{a,bar{c,d}}"), containsInAnyOrder("fooa", "foobarc", "foobard"));
    }
}
