package org.dcache.webadmin.view.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DiskSpaceUnitTest {

    private static final long TEN_MIBIBYTES_IN_BYTES = 10485760L;
    private static final long TEN_MIBIBYTES_IN_MIBIBYTES = 10L;

    @Test
    public void testByteToMibiByte() {
        assertEquals(TEN_MIBIBYTES_IN_MIBIBYTES, DiskSpaceUnit.BYTES.convert(
                TEN_MIBIBYTES_IN_BYTES, DiskSpaceUnit.MIBIBYTES));
    }

    @Test
    public void testMibiByteToByte() {
        assertEquals(TEN_MIBIBYTES_IN_BYTES, DiskSpaceUnit.MIBIBYTES.convert(
                TEN_MIBIBYTES_IN_MIBIBYTES, DiskSpaceUnit.BYTES));
    }

    @Test
    public void testMibiByteToMibiByte() {
        assertEquals(TEN_MIBIBYTES_IN_MIBIBYTES, DiskSpaceUnit.MIBIBYTES.convert(
                TEN_MIBIBYTES_IN_MIBIBYTES, DiskSpaceUnit.MIBIBYTES));
    }
}
