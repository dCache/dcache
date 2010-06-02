package org.dcache.tests.util;

import org.junit.Test;
import static org.junit.Assert.*;

import dmg.util.Args;


public class ArgsTest {


    @Test
    public void testNoArgs() {

        Args args =  new Args("");

        assertEquals("Invalid number of arguments", 0, args.argc() );

    }


    @Test
    public void testWithArgs() {

        Args args =  new Args("arg1 aarg2 arg3 arg4 arg5");

        assertEquals("Invalid number of arguments", 5, args.argc() );

    }


    @Test
    public void testWithOps() {

        Args args =  new Args("-opt1 -opt2 -opt3");

        assertEquals("Invalid number of arguments", 0, args.argc() );
        assertEquals("Invalid number of options", 3, args.options().size() );

    }


    @Test
    public void testWithOpsArgs() {

        Args args =  new Args("-opt1 -opt2 -opt3 arg1 aarg2 arg3 arg4 arg5");

        assertEquals("Invalid number of arguments", 5, args.argc() );
        assertEquals("Invalid number of options", 3, args.options().size() );

    }


    @Test
    public void testDoubleOpts() {

        Args args =  new Args("-opt1 -opt2 -opt3 -opt4=opt4-1 -opt4=opt4-2");

        assertEquals("Invalid number of arguments", 0, args.argc() );
        assertEquals("Invalid number of options", 4, args.options().size() );

    }

    @Test
    public void testLocationManager()
    {
        Args args = new Args("do -serial=118 * nl d:dCacheDomain c:dCacheDomain");
        assertEquals(5, args.argc());
        assertEquals(1, args.optc());
        assertEquals("do", args.argv(0));
        assertEquals("*", args.argv(1));
        assertEquals("nl", args.argv(2));
        assertEquals("d:dCacheDomain", args.argv(3));
        assertEquals("c:dCacheDomain", args.argv(4));
        assertEquals("118", args.getOpt("serial"));

        assertEquals(args, new Args(args.toString()));
    }

    @Test
    public void testDoubleQuoteArgument()
    {
        Args args = new Args("-foo \"bar\"");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testDoubleQuoteArgumentWithEscape()
    {
        Args args = new Args("-foo \"b\\\"a\\\"r\"");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("b\"a\"r", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testDoubleQuoteArgumentWithSingleQuote()
    {
        Args args = new Args("-foo \"bar''\"");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar''", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testDoubleQuoteInsideArgument()
    {
        Args args = new Args("-foo b\"a\"r");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testDoubleQuoteOption()
    {
        Args args = new Args("\"-foo\" bar");
        assertEquals(2, args.argc());
        assertEquals("-foo", args.argv(0));
        assertEquals("bar", args.argv(1));
        assertFalse(args.isOneCharOption('-'));
        assertFalse(args.isOneCharOption('f'));
        assertFalse(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testDoubleQuoteOptionValue()
    {
        Args args = new Args("-foo=\"bar bar\"");
        assertEquals(0, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar bar", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertFalse(args.isOneCharOption('f'));
        assertFalse(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testSingleQuoteArgument()
    {
        Args args = new Args("-foo 'bar'");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testSingleQuoteArgumentWithEscape()
    {
        Args args = new Args("-foo 'b\\\"a\\\"r'");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("b\\\"a\\\"r", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testSingleQuoteArgumentWithDoubleQuote()
    {
        Args args = new Args("-foo 'bar\"\"'");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar\"\"", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testSingleQuoteInsideArgument()
    {
        Args args = new Args("-foo b'a'r");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testSingleQuoteOption()
    {
        Args args = new Args("'-foo' bar");
        assertEquals(2, args.argc());
        assertEquals(0, args.optc());
        assertEquals("-foo", args.argv(0));
        assertEquals("bar", args.argv(1));
        assertFalse(args.isOneCharOption('-'));
        assertFalse(args.isOneCharOption('f'));
        assertFalse(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testSingleQuoteOptionValue()
    {
        Args args = new Args("-foo='bar bar'");
        assertEquals(0, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar bar", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertFalse(args.isOneCharOption('f'));
        assertFalse(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testEscapedOption()
    {
        Args args = new Args("\\-foo");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("-foo", args.argv(0));
        assertFalse(args.isOneCharOption('-'));
        assertFalse(args.isOneCharOption('f'));
        assertFalse(args.isOneCharOption('o'));
    }

    @Test
    public void testEscapedSpaceOption()
    {
        Args args = new Args("-foo=bar\\ bar");
        assertEquals(0, args.argc());
        assertEquals(1, args.optc());
        assertEquals("bar bar", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertFalse(args.isOneCharOption('f'));
        assertFalse(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
    }

    @Test
    public void testEscapedSpaceArgument()
    {
        Args args = new Args("bar\\ bar");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("bar bar", args.argv(0));
    }

    @Test
    public void testEscapedBackslash()
    {
        Args args = new Args("bar\\\\bar");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("bar\\bar", args.argv(0));
    }

    @Test
    public void testTrailingEscape()
    {
        Args args = new Args("bar\\");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("bar", args.argv(0));
    }

    @Test
    public void testEmptyOptionKey()
    {
        Args args = new Args("-");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("-", args.argv(0));
        assertFalse(args.isOneCharOption('-'));
    }

    @Test
    public void testTrailingHyphen()
    {
        Args args = new Args("aa bb -");
        assertEquals(3, args.argc());
        assertEquals(0, args.optc());
        assertEquals("aa", args.argv(0));
        assertEquals("bb", args.argv(1));
        assertEquals("-", args.argv(2));
        assertFalse(args.isOneCharOption('-'));
    }

    @Test
    public void testOptionKeyStartingWithAssignment()
    {
        Args args = new Args("-=bar");
        assertEquals(0, args.argc());
        assertEquals(1, args.optc());
        assertEquals("", args.getOpt("=bar"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('='));
        assertTrue(args.isOneCharOption('b'));
        assertTrue(args.isOneCharOption('a'));
        assertTrue(args.isOneCharOption('r'));
    }

    @Test
    public void testEndOfOptions()
    {
        Args args = new Args("-foo -- -bar");
        assertEquals(1, args.argc());
        assertEquals(1, args.optc());
        assertEquals("-bar", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertFalse(args.isOneCharOption('b'));
        assertFalse(args.isOneCharOption('a'));
        assertFalse(args.isOneCharOption('r'));
    }

    @Test
    public void testEscapedEndOfOptions()
    {
        Args args = new Args("-foo \\-- -bar");
        assertEquals(1, args.argc());
        assertEquals(2, args.optc());
        assertEquals("--", args.argv(0));
        assertEquals("", args.getOpt("foo"));
        assertEquals("", args.getOpt("bar"));
        assertFalse(args.isOneCharOption('-'));
        assertTrue(args.isOneCharOption('f'));
        assertTrue(args.isOneCharOption('o'));
        assertTrue(args.isOneCharOption('b'));
        assertTrue(args.isOneCharOption('a'));
        assertTrue(args.isOneCharOption('r'));
    }
}
