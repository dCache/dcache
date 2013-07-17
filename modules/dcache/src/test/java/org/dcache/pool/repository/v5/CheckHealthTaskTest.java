package org.dcache.pool.repository.v5;

import org.junit.Test;

import static org.junit.Assert.*;

public class CheckHealthTaskTest
{
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
    public void testEmptyCommand()
    {
        String[] command = new CheckHealthTask.Scanner("").scan();
        assertEquals(0, command.length);
    }

    @Test
    public void testWithNoArgs()
    {
        String[] command = new CheckHealthTask.Scanner("command").scan();
        assertArrayEquals(new String[]{"command"}, command);
    }


    @Test
    public void testWithArgs()
    {
        String[] command = new CheckHealthTask.Scanner("command arg1 arg2 arg3 arg4 arg5").scan();
        assertArrayEquals(new String[]{"command", "arg1", "arg2", "arg3", "arg4", "arg5"}, command);
    }

    @Test
    public void testDoubleQuoteArgument()
    {
        String[] command = new CheckHealthTask.Scanner("\"foo bar\" bla").scan();
        assertArrayEquals(new String[]{"foo bar", "bla"}, command);
    }

    @Test
    public void testDoubleQuoteArgumentWithEscape()
    {
        String[] command = new CheckHealthTask.Scanner("foo \"b\\\"a\\\"r\"").scan();
        assertArrayEquals(new String[]{"foo", "b\"a\"r"}, command);
    }

    @Test
    public void testDoubleQuoteInsideArgument()
    {
        String[] command = new CheckHealthTask.Scanner("foo b\"a\"r").scan();
        assertArrayEquals(new String[]{"foo", "bar"}, command);
    }

    @Test
    public void testSingleQuoteArgument()
    {
        String[] command = new CheckHealthTask.Scanner("foo 'bar bla'").scan();
        assertArrayEquals(new String[]{"foo", "bar bla"}, command);
    }

    @Test
    public void testEscapedSpaceArgument()
    {
        String[] command = new CheckHealthTask.Scanner("bar\\ bar").scan();
        assertArrayEquals(new String[]{"bar bar"}, command);
    }

    @Test
    public void testEscapedBackslash()
    {
        String[] command = new CheckHealthTask.Scanner("bar\\\\bar").scan();
        assertArrayEquals(new String[]{"bar\\bar"}, command);
    }
}
