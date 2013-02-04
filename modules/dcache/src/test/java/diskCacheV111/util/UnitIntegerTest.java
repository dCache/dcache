package diskCacheV111.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class UnitIntegerTest
{
    private final static long TEBI = (1L << 40);
    private final static long GIBI = (1L << 30);
    private final static long MEBI = (1L << 20);
    private final static long KIBI = (1L << 10);

    @Test
    public void testParsing()
    {
        assertEquals(-1, UnitInteger.parseUnitLong("-1"));
        assertEquals(0, UnitInteger.parseUnitLong("0"));
        assertEquals(1, UnitInteger.parseUnitLong("1"));

        assertEquals(-KIBI, UnitInteger.parseUnitLong("-1k"));
        assertEquals(0, UnitInteger.parseUnitLong("0k"));
        assertEquals(KIBI, UnitInteger.parseUnitLong("1k"));
        assertEquals(-KIBI, UnitInteger.parseUnitLong("-1K"));
        assertEquals(0, UnitInteger.parseUnitLong("0K"));
        assertEquals(KIBI, UnitInteger.parseUnitLong("1K"));

        assertEquals(-MEBI, UnitInteger.parseUnitLong("-1m"));
        assertEquals(0, UnitInteger.parseUnitLong("0m"));
        assertEquals(MEBI, UnitInteger.parseUnitLong("1m"));
        assertEquals(-MEBI, UnitInteger.parseUnitLong("-1M"));
        assertEquals(0, UnitInteger.parseUnitLong("0M"));
        assertEquals(MEBI, UnitInteger.parseUnitLong("1M"));

        assertEquals(-GIBI, UnitInteger.parseUnitLong("-1g"));
        assertEquals(0, UnitInteger.parseUnitLong("0g"));
        assertEquals(GIBI, UnitInteger.parseUnitLong("1g"));
        assertEquals(-GIBI, UnitInteger.parseUnitLong("-1G"));
        assertEquals(0, UnitInteger.parseUnitLong("0G"));
        assertEquals(GIBI, UnitInteger.parseUnitLong("1G"));

        assertEquals(-TEBI, UnitInteger.parseUnitLong("-1t"));
        assertEquals(0, UnitInteger.parseUnitLong("0t"));
        assertEquals(TEBI, UnitInteger.parseUnitLong("1t"));
        assertEquals(-TEBI, UnitInteger.parseUnitLong("-1T"));
        assertEquals(0, UnitInteger.parseUnitLong("0T"));
        assertEquals(TEBI, UnitInteger.parseUnitLong("1T"));
    }

    @Test
    public void testToString()
    {
        assertEquals("0", UnitInteger.toUnitString(0));
        assertEquals("1", UnitInteger.toUnitString(1));
        assertEquals("1K", UnitInteger.toUnitString(KIBI));
        assertEquals("1M", UnitInteger.toUnitString(MEBI));
        assertEquals("1G", UnitInteger.toUnitString(GIBI));
        assertEquals("1T", UnitInteger.toUnitString(TEBI));
    }
}
