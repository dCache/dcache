package dmg.util.logback;

import org.slf4j.Logger;
import ch.qos.logback.classic.Level;
import org.junit.*;
import static org.junit.Assert.*;

public class FilterThresholdsTest
{
    private final static String FILTER1 = "filter1";
    private final static String FILTER2 = "filter2";
    private final static String FILTER3 = "filter3";

    private final static LoggerName LOGGER = LoggerName.getInstance("foo");
    private final static LoggerName CHILD = LoggerName.getInstance("foo.bar");

    private final static Level LEVEL1 = Level.DEBUG;
    private final static Level LEVEL2 = Level.INFO;
    private final static Level LEVEL3 = Level.ERROR;

    private FilterThresholds _root;
    private FilterThresholds _inherited;

    @Before
    public void setup()
    {
        _root = new FilterThresholds();
        _inherited = new FilterThresholds(_root);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testAddFilterNull()
    {
        _root.addFilter(null);
    }

    @Test
    public void testAddGetFilters()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        assertTrue(_root.getFilters().contains(FILTER1));
        assertTrue(_root.getFilters().contains(FILTER2));
        assertEquals(2, _root.getFilters().size());
    }

    @Test
    public void testAddGetFiltersInherited()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _inherited.addFilter(FILTER3);
        assertTrue(_root.getFilters().contains(FILTER1));
        assertTrue(_root.getFilters().contains(FILTER2));
        assertFalse(_root.getFilters().contains(FILTER3));
        assertTrue(_inherited.getFilters().contains(FILTER1));
        assertTrue(_inherited.getFilters().contains(FILTER2));
        assertTrue(_inherited.getFilters().contains(FILTER3));
        assertEquals(2, _root.getFilters().size());
        assertEquals(3, _inherited.getFilters().size());
    }

    @Test
    public void testAddHasFilters()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        assertTrue(_root.hasFilter(FILTER1));
        assertTrue(_root.hasFilter(FILTER2));
    }

    @Test
    public void testAddHasFiltersInherited()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _inherited.addFilter(FILTER3);
        assertTrue(_root.hasFilter(FILTER1));
        assertTrue(_root.hasFilter(FILTER2));
        assertTrue(_inherited.hasFilter(FILTER1));
        assertTrue(_inherited.hasFilter(FILTER2));
        assertTrue(_inherited.hasFilter(FILTER3));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetThresholdWithUndefinedFilter()
    {
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetThresholdWithNull1()
    {
        _root.setThreshold(null, FILTER1, LEVEL1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetThresholdWithNull2()
    {
        _root.setThreshold(LOGGER, null, LEVEL1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetThresholdWithNull3()
    {
        _root.setThreshold(LOGGER, FILTER1, null);
    }

    @Test
    public void testSetThreshold()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);
        _root.setThreshold(LOGGER, FILTER2, LEVEL2);
        assertEquals(LEVEL1, _root.get(LOGGER, FILTER1));
        assertEquals(LEVEL2, _root.get(LOGGER, FILTER2));
    }

    @Test
    public void testRemove()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);
        _root.setThreshold(LOGGER, FILTER2, LEVEL2);
        _root.remove(LOGGER, FILTER1);
        assertNull(_root.get(LOGGER, FILTER1));
        assertEquals(LEVEL2, _root.get(LOGGER, FILTER2));
    }

    @Test
    public void testClear()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);
        _root.setThreshold(LOGGER, FILTER2, LEVEL2);
        _root.clear();
        assertNull(_root.get(LOGGER, FILTER1));
        assertNull(_root.get(LOGGER, FILTER2));
        assertTrue(_root.hasFilter(FILTER1));
        assertTrue(_root.hasFilter(FILTER2));
    }

    @Test
    public void testGetInheritedMap()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);
        _root.setThreshold(LOGGER, FILTER2, LEVEL2);
        _inherited.setThreshold(LOGGER, FILTER2, LEVEL3);

        assertEquals(LEVEL1, _root.getInheritedMap(LOGGER).get(FILTER1));
        assertEquals(LEVEL2, _root.getInheritedMap(LOGGER).get(FILTER2));
        assertEquals(LEVEL1, _inherited.getInheritedMap(LOGGER).get(FILTER1));
        assertEquals(LEVEL3, _inherited.getInheritedMap(LOGGER).get(FILTER2));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getInheritedMap(LOGGER).get(FILTER1));
        assertEquals(LEVEL2, _root.getInheritedMap(LOGGER).get(FILTER2));
        assertEquals(LEVEL1, _inherited.getInheritedMap(LOGGER).get(FILTER1));
        assertEquals(LEVEL3, _inherited.getInheritedMap(LOGGER).get(FILTER2));
    }

    @Test
    public void testInheritance1()
    {
        _root.addFilter(FILTER1);
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);

        assertEquals(LEVEL1, _root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, FILTER1));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, FILTER1));
    }

    @Test
    public void testInheritance2()
    {
        _root.addFilter(FILTER1);
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);
        _root.setThreshold(CHILD, FILTER1, LEVEL2);
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL2, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL2, _inherited.getThreshold(CHILD, FILTER1));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL2, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL2, _inherited.getThreshold(CHILD, FILTER1));
    }

    @Test
    public void testInheritance3()
    {
        _root.addFilter(FILTER1);
        _root.setThreshold(LOGGER, FILTER1, LEVEL1);
        _inherited.setThreshold(CHILD, FILTER1, LEVEL3);
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL3, _inherited.getThreshold(CHILD, FILTER1));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL3, _inherited.getThreshold(CHILD, FILTER1));
    }

    @Test
    public void testInheritance4()
    {
        _root.addFilter(FILTER1);
        _root.setThreshold(CHILD, FILTER1, LEVEL1);
        _inherited.setThreshold(LOGGER, FILTER1, LEVEL3);

        assertNull(_root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL3, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, FILTER1));

        // Test twice to check caching
        assertNull(_root.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _root.getThreshold(CHILD, FILTER1));
        assertEquals(LEVEL3, _inherited.getThreshold(LOGGER, FILTER1));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD, FILTER1));
    }

    @Test
    public void testMinimumThreshold1()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _root.setThreshold(CHILD, FILTER1, LEVEL2);
        _inherited.setThreshold(LOGGER, FILTER1, LEVEL3);

        assertNull(_root.getThreshold(LOGGER));
        assertEquals(LEVEL2, _root.getThreshold(CHILD));
        assertEquals(LEVEL3, _inherited.getThreshold(LOGGER));
        assertEquals(LEVEL2, _inherited.getThreshold(CHILD));

        // Test twice to check caching
        assertNull(_root.getThreshold(LOGGER));
        assertEquals(LEVEL2, _root.getThreshold(CHILD));
        assertEquals(LEVEL3, _inherited.getThreshold(LOGGER));
        assertEquals(LEVEL2, _inherited.getThreshold(CHILD));
    }

    @Test
    public void testMinimumThreshold2()
    {
        _root.addFilter(FILTER1);
        _root.addFilter(FILTER2);
        _root.setThreshold(CHILD, FILTER1, LEVEL2);
        _root.setThreshold(LOGGER, FILTER2, LEVEL1);
        _inherited.setThreshold(LOGGER, FILTER1, LEVEL3);

        assertEquals(LEVEL1, _root.getThreshold(LOGGER));
        assertEquals(LEVEL1, _root.getThreshold(CHILD));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD));

        // Test twice to check caching
        assertEquals(LEVEL1, _root.getThreshold(LOGGER));
        assertEquals(LEVEL1, _root.getThreshold(CHILD));
        assertEquals(LEVEL1, _inherited.getThreshold(LOGGER));
        assertEquals(LEVEL1, _inherited.getThreshold(CHILD));
    }
}
