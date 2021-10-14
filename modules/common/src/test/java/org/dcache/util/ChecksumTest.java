package org.dcache.util;

import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.MD4_TYPE;
import static org.dcache.util.ChecksumType.MD5_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * Test the Checksum class.
 */
public class ChecksumTest {

    @Test
    public void testValidAdler32NoPaddingNeeded() {
        Checksum checksum = new Checksum(ADLER32, "00675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32FromBytes() {
        byte[] value = {0x00, 0x67, 0x5a, (byte) 0xf7};

        Checksum checksum = new Checksum(ADLER32, value);

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32NoPaddingNeededUpperCase() {
        Checksum checksum = new Checksum(ADLER32, "00675AF7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32PaddingNeeded() {
        Checksum checksum = new Checksum(ADLER32, "675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32PaddingNeededByBytes() {
        byte[] value = {0x67, 0x5a, (byte) 0xf7};

        Checksum checksum = new Checksum(ADLER32, value);

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAdler32() {
        new Checksum(ADLER32, "INVALID");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLongAdler32() {
        new Checksum(ADLER32, "65e311e678cfc58a1c8c740452b3708");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLongAdler32ByBytes() {
        byte[] value = {0x00, 0x67, 0x5a, (byte) 0xf7, (byte) 0xf7};
        new Checksum(ADLER32, value);
    }

    @Test(expected = NullPointerException.class)
    public void testNullStringAdler32() {
        new Checksum(ADLER32, (String) null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullByteArrayAdler32() {
        new Checksum(ADLER32, (byte[]) null);
    }

    @Test
    public void testValidMD5FromString() {
        Checksum checksum = new Checksum(MD5_TYPE, "65e311e678cfc58a1c8c740452b3708f");

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.toString(), equalTo("2:65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD5_TYPE));
    }

    @Test
    public void testValidMD5FromBytes() {
        byte[] value = {0x65, (byte) 0xe3, 0x11, (byte) 0xe6, 0x78,
              (byte) 0xcf, (byte) 0xc5, (byte) 0x8a, 0x1c, (byte) 0x8c, 0x74, 0x04,
              0x52, (byte) 0xb3, 0x70, (byte) 0x8f};

        Checksum checksum = new Checksum(MD5_TYPE, value);

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.toString(), equalTo("2:65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD5_TYPE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooShortMD5() {
        new Checksum(MD5_TYPE, "65e311e678cfc58a1c8c740452b3708");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooShortMD5ByBytes() {
        byte[] value = {0x65, (byte) 0xe3, 0x11, (byte) 0xe6, 0x78,
              (byte) 0xcf, (byte) 0xc5, (byte) 0x8a, 0x1c, (byte) 0x8c, 0x74, 0x04,
              0x52, (byte) 0xb3, 0x70};

        new Checksum(MD5_TYPE, value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLongMD5() {
        new Checksum(MD5_TYPE, "65e311e678cfc58a1c8c740452b3708ff");
    }


    @Test(expected = IllegalArgumentException.class)
    public void testTooLongMD5ByBytes() {
        byte[] value = {0x65, (byte) 0xe3, 0x11, (byte) 0xe6, 0x78,
              (byte) 0xcf, (byte) 0xc5, (byte) 0x8a, 0x1c, (byte) 0x8c, 0x74, 0x04,
              0x52, (byte) 0xb3, 0x70, (byte) 0x8f, (byte) 0x8f};

        new Checksum(MD5_TYPE, value);
    }


    @Test
    public void testValidMD4() {
        Checksum checksum = new Checksum(MD4_TYPE, "65e311e678cfc58a1c8c740452b3708f");

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.toString(), equalTo("3:65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD4_TYPE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooShortMD4() {
        new Checksum(MD4_TYPE, "65e311e678cfc58a1c8c740452b3708");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLongMD4() {
        new Checksum(MD4_TYPE, "65e311e678cfc58a1c8c740452b3708ff");
    }

    @Test(expected = NullPointerException.class)
    public void testParseChecksumWithNull() {
        Checksum.parseChecksum(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseChecksumWithInvalidString() {
        Checksum.parseChecksum("00675af7");
    }

    @Test
    public void testParseChecksumForAdler32NoPaddingNeeded() {
        Checksum checksum = Checksum.parseChecksum("1:00675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testParseChecksumForAdler32PaddingNeeded() {
        Checksum checksum = Checksum.parseChecksum("1:675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testParseChecksumForMD5() {
        Checksum checksum = Checksum.parseChecksum("2:65e311e678cfc58a1c8c740452b3708f");

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD5_TYPE));
    }

    @Test
    public void testParsedZeroPrefixStringEqual() {
        Checksum c1 = Checksum.parseChecksum("1:00675af7");
        Checksum c2 = Checksum.parseChecksum("1:675af7");

        assertThat(c1, equalTo(c2));
    }

    @Test
    public void testDifferentValueSameType() {
        Checksum c1 = Checksum.parseChecksum("1:00675af7");
        Checksum c2 = Checksum.parseChecksum("1:00675af8");

        assertThat(c1, not(equalTo(c2)));
    }

    @Test
    public void testSameValueDifferType() {
        Checksum c1 = Checksum.parseChecksum("2:65e311e678cfc58a1c8c740452b3708f");
        Checksum c2 = Checksum.parseChecksum("3:65e311e678cfc58a1c8c740452b3708f");

        assertThat(c1, not(equalTo(c2)));
    }
}
