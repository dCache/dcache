package org.dcache.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class BooleansTest {

    @Test
    public void testYes() {
        assertEquals("yes", Booleans.toYesNoString(true));
    }

    @Test
    public void testNo() {
        assertEquals("no", Booleans.toYesNoString(false));
    }

    @Test
    public void testOn() {
        assertEquals("on", Booleans.toOnOffString(true));
    }

    @Test
    public void testEnable() {
        assertEquals("enable", Booleans.toEnableDisableString(true));
    }

    @Test
    public void testOff() {
        assertEquals("off", Booleans.toOnOffString(false));
    }

    @Test
    public void testTrue() {
        assertEquals("true", Booleans.toTrueFalseString(true));
    }

    @Test
    public void testFalse() {
        assertEquals("false", Booleans.toTrueFalseString(false));
    }

    @Test
    public void testDisable() {
        assertEquals("disable", Booleans.toEnableDisableString(false));
    }

    @Test
    public void testTrueFromString() {
        assertTrue(Booleans.of("yes"));
        assertTrue(Booleans.of("on"));
        assertTrue(Booleans.of("true"));
        assertTrue(Booleans.of("enable"));
    }

    @Test
    public void testFalseFromString() {
        assertFalse(Booleans.of("no"));
        assertFalse(Booleans.of("off"));
        assertFalse(Booleans.of("false"));
        assertFalse(Booleans.of("disable"));
    }

    @Test
    public void testFalseFromStringIgnoreCase() {
        assertFalse(Booleans.of("No"));
        assertFalse(Booleans.of("OFF"));
        assertFalse(Booleans.of("faLse"));
        assertFalse(Booleans.of("DisAblE"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromBadString() {
        assertTrue(Booleans.of("bla"));
    }
}
