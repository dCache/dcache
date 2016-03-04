package diskCacheV111.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DiskSpaceTest
{
    private final static long TEBI = (1L << 40);
    private final static long GIBI = (1L << 30);
    private final static long MEBI = (1L << 20);
    private final static long KIBI = (1L << 10);

    @Test
    public void testParsing()
    {
        assertEquals(-1, new DiskSpace("-1").longValue());
        assertEquals(0, new DiskSpace("0").longValue());
        assertEquals(1, new DiskSpace("1").longValue());

        assertEquals(-KIBI, new DiskSpace("-1k").longValue());
        assertEquals(0, new DiskSpace("0k").longValue());
        assertEquals(KIBI, new DiskSpace("1k").longValue());
        assertEquals(-KIBI, new DiskSpace("-1K").longValue());
        assertEquals(0, new DiskSpace("0K").longValue());
        assertEquals(KIBI, new DiskSpace("1K").longValue());

        assertEquals(-MEBI, new DiskSpace("-1m").longValue());
        assertEquals(0, new DiskSpace("0m").longValue());
        assertEquals(MEBI, new DiskSpace("1m").longValue());
        assertEquals(-MEBI, new DiskSpace("-1M").longValue());
        assertEquals(0, new DiskSpace("0M").longValue());
        assertEquals(MEBI, new DiskSpace("1M").longValue());

        assertEquals(-GIBI, new DiskSpace("-1g").longValue());
        assertEquals(0, new DiskSpace("0g").longValue());
        assertEquals(GIBI, new DiskSpace("1g").longValue());
        assertEquals(-GIBI, new DiskSpace("-1G").longValue());
        assertEquals(0, new DiskSpace("0G").longValue());
        assertEquals(GIBI, new DiskSpace("1G").longValue());

        assertEquals(-TEBI, new DiskSpace("-1t").longValue());
        assertEquals(0, new DiskSpace("0t").longValue());
        assertEquals(TEBI, new DiskSpace("1t").longValue());
        assertEquals(-TEBI, new DiskSpace("-1T").longValue());
        assertEquals(0, new DiskSpace("0T").longValue());
        assertEquals(TEBI, new DiskSpace("1T").longValue());
    }

    @Test
    public void testToString()
    {
        assertEquals("0", DiskSpace.toUnitString(0));
        assertEquals("1", DiskSpace.toUnitString(1));
        assertEquals("1K", DiskSpace.toUnitString(KIBI));
        assertEquals("1M", DiskSpace.toUnitString(MEBI));
        assertEquals("1G", DiskSpace.toUnitString(GIBI));
        assertEquals("1T", DiskSpace.toUnitString(TEBI));
    }
}
