package org.dcache.util;

import org.junit.Test;
import static org.junit.Assert.*;

public class ChecksumTest {

    private final byte[] _input = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    @Test
    public void testZeroPrefixString() throws Exception {

        Checksum c1 = Checksum.parseChecksum("1:675af7");
        Checksum c2 = Checksum.parseChecksum("1:00675af7");

        assertTrue("1:675af7 and 1:00675af7 have to be equal", c1.equals(c2));

    }

    @Test
    public void testSameValueDifferType() throws Exception {

        Checksum c1 = Checksum.parseChecksum("1:00675af7");
        Checksum c2 = Checksum.parseChecksum("2:00675af7");

        assertFalse("Same value but different type should be differ", c1.equals(c2));

    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore
                .main("org.dcache.tests.util.ChecksumTestCase");
    }
}
