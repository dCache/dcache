package org.dcache.tests.pool.migration;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import org.dcache.pool.migration.MapContextWithConstants;


public class MapContextWithConstantsTest
{
    private static final String NAME1 = "name1";
    private static final String VALUE1 = "value1";

    private MapContextWithConstants _context;

    @Before
    public void setup()
    {
        _context = new MapContextWithConstants();
    }

    @Test
    public void testMissingVariable()
    {
        assertFalse(_context.has(NAME1));
        assertNull(_context.get(NAME1));
    }

    @Test
    public void testSetVariable()
    {
        _context.set(NAME1, VALUE1);
        assertTrue(_context.has(NAME1));
        assertEquals(VALUE1, _context.get(NAME1));
    }

    @Test
    public void testAddConstant()
    {
        _context.addConstant(NAME1, VALUE1);
        assertTrue(_context.has(NAME1));
        assertEquals(VALUE1, _context.get(NAME1));
    }

    @Test(expected=IllegalStateException.class)
    public void testModifyConstant()
    {
        _context.addConstant(NAME1, VALUE1);
        _context.set(NAME1, VALUE1);
    }
}