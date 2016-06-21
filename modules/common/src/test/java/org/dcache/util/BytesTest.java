package org.dcache.util;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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

    @Test(expected=IllegalArgumentException.class)
    public void testPutIntIntoSmall() {
        Bytes.putInt(_b, 6, 0);
    }

    @Test
    public void testPutLongExact() {
        Bytes.putLong(_b, _b.length - 8, 0);
    }

    @Test
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

    @Test
    public void testToHexString() {
        byte[] bytes = new byte[]{(byte) 0xf, (byte) 0xf};
        assertEquals("0f0f", Bytes.toHexString(bytes));
    }

    @Test
    public void testToHexString2() {
        byte[] bytes = new byte[]{(byte) 0xff};
        assertEquals("ff", Bytes.toHexString(bytes));
    }

    @Test
    public void testToHexString3() {
        byte[] bytes = new byte[]{(byte) 0x1, (byte) 0xe7};
        assertEquals("01e7", Bytes.toHexString(bytes));
    }
}
