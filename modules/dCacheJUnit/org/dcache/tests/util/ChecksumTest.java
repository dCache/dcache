package org.dcache.tests.util;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import diskCacheV111.util.Checksum;
import diskCacheV111.util.Adler32;

public class ChecksumTest {

    private final byte[] _input = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    @Test
    public void testMessageDigestConstructor() throws Exception {

        Adler32 adler;

        adler = new Adler32();
        adler.update(_input);
        Checksum c1 = new Checksum(1, adler.digest());

        adler = new Adler32();
        Checksum c2 = new Checksum(adler);
        c2.getMessageDigest().update(_input);

        assertEquals(c1, c2);

    }

    @Test
    public void testZeroPrefixString() throws Exception {

        Checksum c1 = new Checksum("1:675af7");
        Checksum c2 = new Checksum("1:00675af7");

        assertTrue("1:675af7 and 1:00675af7 have to be equal", c1.equals(c2));

    }

    @Test
    public void testSameValueDifferType() throws Exception {

        Checksum c1 = new Checksum("1:00675af7");
        Checksum c2 = new Checksum("2:00675af7");

        assertFalse("Same value but different type should be differ", c1.equals(c2));

    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore
                .main("org.dcache.tests.util.ChecksumTestCase");
    }
}
