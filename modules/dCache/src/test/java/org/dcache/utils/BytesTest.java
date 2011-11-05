package org.dcache.utils;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tigran
 */
public class BytesTest {


    private byte[] _b;
    @Before
    public void setUp() {
        _b = new byte[8];
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPutLongIntoSmall() {
        Bytes.putLong(_b, 6, 0);
    }

    public void testPutIntIntoSmall() {
        Bytes.putInt(_b, 6, 0);
    }

    @Test
    public void testPutLongExact() {
        Bytes.putLong(_b, _b.length - 8, 0);
    }

    public void testPutIntExact() {
        Bytes.putInt(_b, _b.length - 4, 0);
    }

    @Test
    public void testPutGetLong() {
        long value = 1717;
        Bytes.putLong(_b, 0, value);
        assertEquals("put/get mismatch", value, Bytes.getLong(_b, 0));
    }

    @Test
    public void testPutGetInt() {
        int value = 1717;
        Bytes.putInt(_b, 0, value);
        assertEquals("put/get mismatch", value, Bytes.getInt(_b, 0));
    }
}