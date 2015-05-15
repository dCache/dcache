package dmg.util.logback;

import ch.qos.logback.classic.Level;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import static org.junit.Assert.*;

public class FilterThresholdSetTest
{
    private final static String APPENDER1 = "appender1";
    private final static String APPENDER2 = "appender2";
    private final static String APPENDER3 = "appender3";

    private final static LoggerName LOGGER = LoggerName.getInstance("foo");
    private final static LoggerName CHILD = LoggerName.getInstance("foo.bar");

    private final static Level LEVEL1 = Level.DEBUG;
    private final static Level LEVEL2 = Level.INFO;
    private final static Level LEVEL3 = Level.ERROR;

    private FilterThresholdSet _root;
    private FilterThresholdSet _inherited;

    @Before
    public void setup()
    {
        _root = new FilterThresholdSet();
        _inherited = new FilterThresholdSet(_root);
    }

    @Test(expected=NullPointerException.class)
    public void testAddFilterNull()
    {
        _root.addAppender(null);
    }

    @Test
    public void testAddGetFilters()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        assertTrue(_root.getAppenders().contains(APPENDER1));
        assertTrue(_root.getAppenders().contains(APPENDER2));
        assertEquals(2, _root.getAppenders().size());
    }

    @Test
    public void testAddGetFiltersInherited()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _inherited.addAppender(APPENDER3);
        assertTrue(_root.getAppenders().contains(APPENDER1));
        assertTrue(_root.getAppenders().contains(APPENDER2));
        assertFalse(_root.getAppenders().contains(APPENDER3));
        assertTrue(_inherited.getAppenders().contains(APPENDER1));
        assertTrue(_inherited.getAppenders().contains(APPENDER2));
        assertTrue(_inherited.getAppenders().contains(APPENDER3));
        assertEquals(2, _root.getAppenders().size());
        assertEquals(3, _inherited.getAppenders().size());
    }

    @Test
    public void testAddHasFilters()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        assertTrue(_root.hasAppender(APPENDER1));
        assertTrue(_root.hasAppender(APPENDER2));
    }

    @Test
    public void testAddHasFiltersInherited()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _inherited.addAppender(APPENDER3);
        assertTrue(_root.hasAppender(APPENDER1));
        assertTrue(_root.hasAppender(APPENDER2));
        assertTrue(_inherited.hasAppender(APPENDER1));
        assertTrue(_inherited.hasAppender(APPENDER2));
        assertTrue(_inherited.hasAppender(APPENDER3));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetThresholdWithUndefinedFilter()
    {
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);
    }

    @Test(expected=NullPointerException.class)
    public void testSetThresholdWithNull1()
    {
        _root.setThreshold(null, APPENDER1, LEVEL1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetThresholdWithNull2()
    {
        _root.setThreshold(LOGGER, null, LEVEL1);
    }

    @Test(expected=NullPointerException.class)
    public void testSetThresholdWithNull3()
    {
        _root.setThreshold(LOGGER, APPENDER1, null);
    }

    @Test
    public void testSetThreshold()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);
        _root.setThreshold(LOGGER, APPENDER2, LEVEL2);
        assertEquals(LEVEL1, _root.get(LOGGER, APPENDER1));
        assertEquals(LEVEL2, _root.get(LOGGER, APPENDER2));
    }

    @Test
    public void testRemove()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);
        _root.setThreshold(LOGGER, APPENDER2, LEVEL2);
        _root.remove(LOGGER, APPENDER1);
        assertNull(_root.get(LOGGER, APPENDER1));
        assertEquals(LEVEL2, _root.get(LOGGER, APPENDER2));
    }

    @Test
    public void testClear()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);
        _root.setThreshold(LOGGER, APPENDER2, LEVEL2);
        _root.clear();
        assertNull(_root.get(LOGGER, APPENDER1));
        assertNull(_root.get(LOGGER, APPENDER2));
        assertTrue(_root.hasAppender(APPENDER1));
        assertTrue(_root.hasAppender(APPENDER2));
    }

    @Test
    public void testGetInheritedMap()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);
        _root.setThreshold(LOGGER, APPENDER2, LEVEL2);
        _inherited.setThreshold(LOGGER, APPENDER2, LEVEL3);

        assertEquals(LEVEL1, _root.getInheritedMap(LOGGER).get(APPENDER1));
        assertEquals(LEVEL2, _root.getInheritedMap(LOGGER).get(APPENDER2));
        assertEquals(LEVEL1, _inherited.getInheritedMap(LOGGER).get(APPENDER1));
        assertEquals(LEVEL3, _inherited.getInheritedMap(LOGGER).get(APPENDER2));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getInheritedMap(LOGGER).get(APPENDER1));
        assertEquals(LEVEL2, _root.getInheritedMap(LOGGER).get(APPENDER2));
        assertEquals(LEVEL1, _inherited.getInheritedMap(LOGGER).get(APPENDER1));
        assertEquals(LEVEL3, _inherited.getInheritedMap(LOGGER).get(APPENDER2));
    }

    @Test
    public void testInheritance1()
    {
        _root.addAppender(APPENDER1);
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);

        assertEquals(LEVEL1, _root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, APPENDER1));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, APPENDER1));
    }

    @Test
    public void testInheritance2()
    {
        _root.addAppender(APPENDER1);
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);
        _root.setThreshold(CHILD, APPENDER1, LEVEL2);
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL2, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL2, _inherited.getThreshold(CHILD, APPENDER1));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL2, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL2, _inherited.getThreshold(CHILD, APPENDER1));
    }

    @Test
    public void testInheritance3()
    {
        _root.addAppender(APPENDER1);
        _root.setThreshold(LOGGER, APPENDER1, LEVEL1);
        _inherited.setThreshold(CHILD, APPENDER1, LEVEL3);
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL3, _inherited.getThreshold(CHILD, APPENDER1));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL3, _inherited.getThreshold(CHILD, APPENDER1));
    }

    @Test
    public void testInheritance4()
    {
        _root.addAppender(APPENDER1);
        _root.setThreshold(CHILD, APPENDER1, LEVEL1);
        _inherited.setThreshold(LOGGER, APPENDER1, LEVEL3);

        assertNull(_root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL3, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, APPENDER1));

        // Test twice to check caching
        assertNull(_root.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, APPENDER1));
        assertEquals(LEVEL3, _inherited.getThreshold(LOGGER, APPENDER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, APPENDER1));
    }

    @Test
    public void testMinimumThreshold1()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _root.setThreshold(CHILD, APPENDER1, LEVEL2);
        _inherited.setThreshold(LOGGER, APPENDER1, LEVEL3);

        assertNull(_root.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL2, _root.getThreshold(getLogger(CHILD)));
        assertEquals(LEVEL3, _inherited.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL2, _inherited.getThreshold(getLogger(CHILD)));

        // Test twice to check caching
        assertNull(_root.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL2, _root.getThreshold(getLogger(CHILD)));
        assertEquals(LEVEL3, _inherited.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL2, _inherited.getThreshold(getLogger(CHILD)));
    }

    @Test
    public void testMinimumThreshold2()
    {
        _root.addAppender(APPENDER1);
        _root.addAppender(APPENDER2);
        _root.setThreshold(CHILD, APPENDER1, LEVEL2);
        _root.setThreshold(LOGGER, APPENDER2, LEVEL1);
        _inherited.setThreshold(LOGGER, APPENDER1, LEVEL3);

        assertEquals(LEVEL1, _root.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL1, _root.getThreshold(getLogger(CHILD)));
        assertEquals(LEVEL1, _inherited.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL1, _inherited.getThreshold(getLogger(CHILD)));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL1, _root.getThreshold(getLogger(CHILD)));
        assertEquals(LEVEL1, _inherited.getThreshold(getLogger(LOGGER)));
        assertEquals(LEVEL1, _inherited.getThreshold(getLogger(CHILD)));
    }

    private Logger getLogger(LoggerName loggerName)
    {
        return new NOPLogger()
        {
            public String getName()
            {
                return loggerName.toString();
            }
        };
    }
}
