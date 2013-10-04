package dmg.util;

import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class ArgsTest {

    private final static String PROPERTY_PREFIX = "-";
    private final static String PROPERTY_KEY_VALUE_SEPARATOR = "=";
    private final static String PROPERTY_SEPARATOR = " ";

    private final static String PROPERTY_INT_KEY = "http-mover-client-idle-timeout";
    private final static String PROPERTY_INT_VALUE = "300";
    private final static int PROPERTY_INT_EXPECTED = 300;

    private final static String PROPERTY_LONG_KEY = "http-mover-connection-max-memory";
    private final static String PROPERTY_LONG_VALUE = "4294967297";
    private final static long PROPERTY_LONG_EXPECTED = 4294967297L;

    private final static String PROPERTY_DOUBLE_KEY = "fault-tolerance";
    private final static String PROPERTY_DOUBLE_VALUE = "0.00417";
    private final static double PROPERTY_DOUBLE_EXPECTED = 0.00417;

    private final static String PROPERTY_STRING_KEY = "xrootd-authn-plugin";
    private final static String PROPERTY_STRING_VALUE = "gsi";
    private final static String PROPERTY_STRING_EXPECTED = PROPERTY_STRING_VALUE;

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
    public void testDefinedOpsDefault() {
        Args args = new Args("-opt=foo");
        assertEquals("specified opstion not detected", "foo", args.getOption("opt", "bar"));
    }

    @Test
    public void testNotDefinedOpsDefault() {
        Args args = new Args("-opt=foo");
        assertEquals("default option is not detected", "bar", args.getOption("someopt", "bar"));
    }

    @Test
    public void testDoubleOpts() {

        Args args =  new Args("-opt1 -opt2 -opt3 -opt4=opt4-1 -opt4=opt4-2");

        assertEquals("Invalid number of arguments", 0, args.argc() );
        assertEquals("Invalid number of options", 5, args.options().size() );
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

        assertTrue(args.hasOption("serial"));

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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertFalse(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertFalse(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertFalse(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
    }

    @Test
    public void testEscapedSpaceArgument()
    {
        Args args = new Args("bar\\ bar");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("bar bar", args.argv(0));
        assertFalse(args.hasOption("bar"));
    }

    @Test
    public void testEscapedBackslash()
    {
        Args args = new Args("bar\\\\bar");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("bar\\bar", args.argv(0));
        assertFalse(args.hasOption("bar"));
    }

    @Test
    public void testTrailingEscape()
    {
        Args args = new Args("bar\\");
        assertEquals(1, args.argc());
        assertEquals(0, args.optc());
        assertEquals("bar", args.argv(0));
        assertFalse(args.hasOption("bar"));
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
        assertFalse(args.hasOption("aa"));
        assertFalse(args.hasOption("bb"));
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
        assertFalse(args.hasOption("foo"));
        assertTrue(args.hasOption("=bar"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertFalse(args.hasOption("bar"));
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
        assertTrue(args.hasOption("foo"));
        assertTrue(args.hasOption("bar"));
    }

    @Test
    public void testIntOption()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_INT_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_INT_VALUE;
        Args args = new Args(argsString);

        int parseResult = args.getIntOption(PROPERTY_INT_KEY);
        assertEquals("Parsing of integer option does not match expected result.",
                     PROPERTY_INT_EXPECTED,
                     parseResult);
    }

    @Test
    public void testLongOption()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_LONG_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_LONG_VALUE;

        Args args = new Args(argsString);

        long parseResult = args.getLongOption(PROPERTY_LONG_KEY);
        assertEquals("Parsing of long option does not match expected result.",
                     PROPERTY_LONG_EXPECTED,
                     parseResult);
    }

    @Test
    public void testDoubleOption() {
        String argsString = PROPERTY_PREFIX + PROPERTY_DOUBLE_KEY
                        + PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_DOUBLE_VALUE;

        Args args = new Args(argsString);

        double parseResult = args.getDoubleOption(PROPERTY_DOUBLE_KEY);
        assertEquals("Parsing of double option does not match expected result.",
                        PROPERTY_DOUBLE_EXPECTED, parseResult, 0.0);
    }

    @Test
    public void testNumericAndNonNumericOptions()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_STRING_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_STRING_VALUE +
                            PROPERTY_SEPARATOR + PROPERTY_PREFIX +
                            PROPERTY_INT_KEY + PROPERTY_KEY_VALUE_SEPARATOR +
                            PROPERTY_INT_VALUE + PROPERTY_SEPARATOR +
                            PROPERTY_PREFIX + PROPERTY_LONG_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_LONG_VALUE;

        Args args = new Args(argsString);

        String parsedString = args.getOpt(PROPERTY_STRING_KEY);
        assertEquals("Parsing of string option does not match expected result.",
                     PROPERTY_STRING_EXPECTED,
                     parsedString);

        int parsedInt = args.getIntOption(PROPERTY_INT_KEY);
        assertEquals("Parsing of string option does not match expected result.",
                     PROPERTY_INT_EXPECTED,
                     parsedInt);

        long parsedLong = args.getLongOption(PROPERTY_LONG_KEY);
        assertEquals("Parsing of string option does not match expected result.",
                     PROPERTY_LONG_EXPECTED,
                     parsedLong);
    }

    @Test
    public void testEmptyStringValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_STRING_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR;

        Args args = new Args(argsString);

        String parsedString = args.getOpt(PROPERTY_STRING_KEY);
        assertEquals("Parsing of string option does not match expected result.",
                     "",
                     parsedString);
    }

    @Test(expected=NumberFormatException.class)
    public void testNonNumericIntValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_INT_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_STRING_VALUE;

        Args args = new Args(argsString);
        args.getIntOption(PROPERTY_INT_KEY);
    }

    @Test(expected=NumberFormatException.class)
    public void testIntOverflow()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_INT_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_LONG_VALUE;

        Args args = new Args(argsString);
        args.getIntOption(PROPERTY_INT_KEY);
    }

    @Test(expected=NumberFormatException.class)
    public void testNonNumericLongValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_LONG_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_STRING_VALUE;

        Args args = new Args(argsString);
        args.getIntOption(PROPERTY_LONG_KEY);
    }

    @Test(expected=NoSuchElementException.class)
    public void testUndefinedIntValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_LONG_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_LONG_VALUE;

        Args args = new Args(argsString);
        args.getIntOption(PROPERTY_STRING_KEY);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMissingIntValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_INT_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR;

        Args args = new Args(argsString);

        args.getIntOption(PROPERTY_INT_KEY, PROPERTY_INT_EXPECTED);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMissingLongValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_LONG_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR;

        Args args = new Args(argsString);

        args.getLongOption(PROPERTY_LONG_KEY, PROPERTY_LONG_EXPECTED);
    }

    @Test
    public void testDefaultIntValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_STRING_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_STRING_VALUE;

        Args args = new Args(argsString);

        int parsedInt = args.getIntOption(PROPERTY_INT_KEY,
                                          PROPERTY_INT_EXPECTED);

        assertEquals("Parsing of integer does not match expected result.",
                     PROPERTY_INT_EXPECTED,
                     parsedInt);
    }

    @Test
    public void testDefaultLongValue()
    {
        String argsString = PROPERTY_PREFIX + PROPERTY_STRING_KEY +
                            PROPERTY_KEY_VALUE_SEPARATOR + PROPERTY_STRING_VALUE;

        Args args = new Args(argsString);

        long parsedLong = args.getLongOption(PROPERTY_LONG_KEY,
                                             PROPERTY_LONG_EXPECTED);

        assertEquals("Parsing of long does not match expected result.",
                     PROPERTY_LONG_EXPECTED,
                     parsedLong);
    }

    @Test
    public void testArgumentWithWhitespace()
    {
        String arg1 = "first argument contains space";
        String arg2 = "second";
        Args args = new Args(new String[] {arg1, arg2});
        assertEquals(2, args.argc());
        assertEquals(arg1, args.argv(0));
        assertEquals(arg2, args.argv(1));
    }
}
