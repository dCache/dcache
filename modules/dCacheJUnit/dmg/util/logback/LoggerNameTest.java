package dmg.util.logback;

import static org.junit.Assert.*;

import org.junit.*;

import org.slf4j.Logger;

public class LoggerNameTest
{
    private static final String CHILD_OF_ROOT = "logger";
    private static final String LOGGER = "logger.foo";
    private static final String CHILD1_OF_LOGGER = "logger.foo.child1";
    private static final String CHILD2_OF_LOGGER = "logger.foo$child2";

    @Test
    public void testRootHasCorrectName()
    {
        assertEquals(Logger.ROOT_LOGGER_NAME, LoggerName.ROOT.toString());
    }

    @Test
    public void testGetInstanceOfRoot()
    {
        assertEquals(LoggerName.ROOT, LoggerName.getInstance(Logger.ROOT_LOGGER_NAME));
    }

    @Test
    public void testGetInstanceOfRootIsCaseInsensitive()
    {
        assertEquals(LoggerName.ROOT, LoggerName.getInstance(Logger.ROOT_LOGGER_NAME.toUpperCase()));
        assertEquals(LoggerName.ROOT, LoggerName.getInstance(Logger.ROOT_LOGGER_NAME.toLowerCase()));
    }

    @Test
    public void testGetInstance()
    {
        assertEquals(LOGGER, LoggerName.getInstance(LOGGER).toString());
    }

    @Test
    public void testValueOfRoot()
    {
        assertEquals(LoggerName.ROOT, LoggerName.valueOf(Logger.ROOT_LOGGER_NAME));
    }

    @Test
    public void testValueOfRootIsCaseInsensitive()
    {
        assertEquals(LoggerName.ROOT, LoggerName.valueOf(Logger.ROOT_LOGGER_NAME.toUpperCase()));
        assertEquals(LoggerName.ROOT, LoggerName.valueOf(Logger.ROOT_LOGGER_NAME.toLowerCase()));
    }

    @Test
    public void testGetValueOf()
    {
        assertEquals(LOGGER, LoggerName.valueOf(LOGGER).toString());
    }

    @Test
    public void testEqualsWithNull()
    {
        assertFalse(LoggerName.ROOT.equals(null));
    }

    @Test
    public void testEqualsWithWrongType()
    {
        assertFalse(LoggerName.ROOT.equals(Logger.ROOT_LOGGER_NAME));
    }

    @Test
    public void testEqualsWithDifferent()
    {
        assertFalse(LoggerName.ROOT.equals(LoggerName.getInstance(LOGGER)));
    }

    @Test
    public void testEqualsWithSame()
    {
        assertTrue(LoggerName.ROOT.equals(LoggerName.ROOT));
    }

    @Test
    public void testEqualsWithEquals()
    {
        assertTrue(LoggerName.getInstance(LOGGER).equals(LoggerName.valueOf(LOGGER)));
    }

    @Test
    public void testHashCodeEquals()
    {
        assertEquals(LoggerName.getInstance(LOGGER).hashCode(),
                     LoggerName.getInstance(LOGGER).hashCode());
    }

    @Test
    public void testParentOfRootIsNull()
    {
        assertNull(LoggerName.ROOT.getParent());
    }

    @Test
    public void testParentIsRoot()
    {
        assertEquals(LoggerName.ROOT,
                     LoggerName.getInstance(CHILD_OF_ROOT).getParent());
    }

    @Test
    public void testParentWithDot()
    {
        assertEquals(LoggerName.getInstance(LOGGER),
                     LoggerName.getInstance(CHILD1_OF_LOGGER).getParent());
    }

    @Test
    public void testParentWithDollar()
    {
        assertEquals(LoggerName.getInstance(LOGGER),
                     LoggerName.getInstance(CHILD2_OF_LOGGER).getParent());
    }
}