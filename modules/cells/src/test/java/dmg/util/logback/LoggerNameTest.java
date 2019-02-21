package dmg.util.logback;

import org.junit.Test;
import org.slf4j.LOGGER;

import static org.junit.Assert.*;

public class LOGGERNameTest
{
    private static final String CHILD_OF_ROOT = "LOGGER";
    private static final String LOGGER = "LOGGER.foo";
    private static final String CHILD1_OF_LOGGER = "LOGGER.foo.child1";
    private static final String CHILD2_OF_LOGGER = "LOGGER.foo$child2";

    @Test
    public void testRootHasCorrectName()
    {
        assertEquals(LOGGER.ROOT_LOGGER_NAME, LOGGERName.ROOT.toString());
    }

    @Test
    public void testGetInstanceOfRoot()
    {
        assertEquals(LOGGERName.ROOT, LOGGERName.getInstance(LOGGER.ROOT_LOGGER_NAME));
    }

    @Test
    public void testGetInstanceOfRootIsCaseInsensitive()
    {
        assertEquals(LOGGERName.ROOT, LOGGERName.getInstance(LOGGER.ROOT_LOGGER_NAME.toUpperCase()));
        assertEquals(LOGGERName.ROOT, LOGGERName.getInstance(LOGGER.ROOT_LOGGER_NAME.toLowerCase()));
    }

    @Test
    public void testGetInstance()
    {
        assertEquals(LOGGER, LOGGERName.getInstance(LOGGER).toString());
    }

    @Test
    public void testValueOfRoot()
    {
        assertEquals(LOGGERName.ROOT, LOGGERName.valueOf(LOGGER.ROOT_LOGGER_NAME));
    }

    @Test
    public void testValueOfRootIsCaseInsensitive()
    {
        assertEquals(LOGGERName.ROOT, LOGGERName.valueOf(LOGGER.ROOT_LOGGER_NAME.toUpperCase()));
        assertEquals(LOGGERName.ROOT, LOGGERName.valueOf(LOGGER.ROOT_LOGGER_NAME.toLowerCase()));
    }

    @Test
    public void testGetValueOf()
    {
        assertEquals(LOGGER, LOGGERName.valueOf(LOGGER).toString());
    }

    @Test
    public void testEqualsWithNull()
    {
        assertFalse(LOGGERName.ROOT.equals(null));
    }

    @Test
    public void testEqualsWithWrongType()
    {
        assertFalse(LOGGERName.ROOT.equals(LOGGER.ROOT_LOGGER_NAME));
    }

    @Test
    public void testEqualsWithDifferent()
    {
        assertFalse(LOGGERName.ROOT.equals(LOGGERName.getInstance(LOGGER)));
    }

    @Test
    public void testEqualsWithSame()
    {
        assertTrue(LOGGERName.ROOT.equals(LOGGERName.ROOT));
    }

    @Test
    public void testEqualsWithEquals()
    {
        assertTrue(LOGGERName.getInstance(LOGGER).equals(LOGGERName.valueOf(LOGGER)));
    }

    @Test
    public void testHashCodeEquals()
    {
        assertEquals(LOGGERName.getInstance(LOGGER).hashCode(),
                     LOGGERName.getInstance(LOGGER).hashCode());
    }

    @Test
    public void testParentOfRootIsNull()
    {
        assertNull(LOGGERName.ROOT.getParent());
    }

    @Test
    public void testParentIsRoot()
    {
        assertEquals(LOGGERName.ROOT,
                     LOGGERName.getInstance(CHILD_OF_ROOT).getParent());
    }

    @Test
    public void testParentWithDot()
    {
        assertEquals(LOGGERName.getInstance(LOGGER),
                     LOGGERName.getInstance(CHILD1_OF_LOGGER).getParent());
    }

    @Test
    public void testParentWithDollar()
    {
        assertEquals(LOGGERName.getInstance(LOGGER),
                     LOGGERName.getInstance(CHILD2_OF_LOGGER).getParent());
    }
}
