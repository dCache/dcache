package org.dcache.util;

import org.junit.Test;

import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.MD4_TYPE;
import static org.dcache.util.ChecksumType.MD5_TYPE;
import static org.dcache.util.ChecksumType.SHA1;
import static org.dcache.util.ChecksumType.SHA256;
import static org.dcache.util.ChecksumType.SHA512;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Test the Checksum class.
 */
public class ChecksumTest
{
    @Test
    public void testValidSha1FromString()
    {
        Checksum checksum = new Checksum(SHA1, "e7e46ff10df57532f816596327e98f9597b8c21e");

        assertThat(checksum.getValue(), equalTo("e7e46ff10df57532f816596327e98f9597b8c21e"));
        assertThat(checksum.toString(), equalTo("4:e7e46ff10df57532f816596327e98f9597b8c21e"));
        assertThat(checksum.getType(), equalTo(SHA1));
    }

    @Test
    public void testValidSha1FromBytes()
    {
        byte[] value = {
                (byte)0xe7, (byte)0xe4, (byte)0x6f, (byte)0xf1,
                (byte)0x0d, (byte)0xf5, (byte)0x75, (byte)0x32,
                (byte)0xf8, (byte)0x16, (byte)0x59, (byte)0x63,
                (byte)0x27, (byte)0xe9, (byte)0x8f, (byte)0x95,
                (byte)0x97, (byte)0xb8, (byte)0xc2, (byte)0x1e
        };

        Checksum checksum = new Checksum(SHA1, value);

        assertThat(checksum.getValue(), equalTo("e7e46ff10df57532f816596327e98f9597b8c21e"));
        assertThat(checksum.toString(), equalTo("4:e7e46ff10df57532f816596327e98f9597b8c21e"));
        assertThat(checksum.getType(), equalTo(SHA1));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortSha1FromString()
    {
        new Checksum(SHA1, "e7e46ff10df57532f816596327e98f95");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortSha1FromBytes()
    {
        byte[] value = {(byte)0xe7, (byte)0xe4, (byte)0x6f, (byte)0xf1};
        new Checksum(SHA1, value);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongSha1FromString()
    {
        new Checksum(SHA1, "e7e46ff10df57532f816596327e98f9597b8c21eaf21");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongSha1FromBytes()
    {
        byte[] value = {
                (byte)0xe7, (byte)0xe4, (byte)0x6f, (byte)0xf1,
                (byte)0x0d, (byte)0xf5, (byte)0x75, (byte)0x32,
                (byte)0xf8, (byte)0x16, (byte)0x59, (byte)0x63,
                (byte)0x27, (byte)0xe9, (byte)0x8f, (byte)0x95,
                (byte)0x97, (byte)0xb8, (byte)0xc2, (byte)0x1e,
                (byte)0x31, (byte)0x41
        };
        new Checksum(SHA1, value);
    }

    @Test
    public void testValidSha256FromString()
    {
        Checksum checksum = new Checksum(SHA256, "36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c");

        assertThat(checksum.getValue(), equalTo("36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));
        assertThat(checksum.toString(), equalTo("5:36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));
        assertThat(checksum.getType(), equalTo(SHA256));
    }

    @Test
    public void testValidSha256FromBytes()
    {
        byte[] value = {
                (byte)0x36, (byte)0xeb, (byte)0x9d, (byte)0xc4,
                (byte)0xa1, (byte)0x50, (byte)0xf3, (byte)0xb2,
                (byte)0xe7, (byte)0x79, (byte)0x09, (byte)0x91,
                (byte)0xe5, (byte)0xf9, (byte)0x78, (byte)0xf8,
                (byte)0x87, (byte)0x63, (byte)0x2f, (byte)0x92,
                (byte)0xa2, (byte)0x00, (byte)0x2b, (byte)0x7c,
                (byte)0x55, (byte)0x34, (byte)0x0c, (byte)0x2a,
                (byte)0x9e, (byte)0xf1, (byte)0xc3, (byte)0x1c
        };
        Checksum checksum = new Checksum(SHA256, value);

        assertThat(checksum.getValue(), equalTo("36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));
        assertThat(checksum.toString(), equalTo("5:36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31c"));
        assertThat(checksum.getType(), equalTo(SHA256));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortSha256FromString()
    {
        new Checksum(SHA256, "e7e46ff10df57532f816596327e98f95");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortSha256FromBytes()
    {
        byte[] value = {(byte)0xe7, (byte)0xe4, (byte)0x6f, (byte)0xf1};
        new Checksum(SHA256, value);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongSha256FromString()
    {
        new Checksum(SHA256, "36eb9dc4a150f3b2e7790991e5f978f887632f92a2002b7c55340c2a9ef1c31caf21");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongSha256FromBytes()
    {
        byte[] value = {
                (byte)0x36, (byte)0xeb, (byte)0x9d, (byte)0xc4,
                (byte)0xa1, (byte)0x50, (byte)0xf3, (byte)0xb2,
                (byte)0xe7, (byte)0x79, (byte)0x09, (byte)0x91,
                (byte)0xe5, (byte)0xf9, (byte)0x78, (byte)0xf8,
                (byte)0x87, (byte)0x63, (byte)0x2f, (byte)0x92,
                (byte)0xa2, (byte)0x00, (byte)0x2b, (byte)0x7c,
                (byte)0x55, (byte)0x34, (byte)0x0c, (byte)0x2a,
                (byte)0x9e, (byte)0xf1, (byte)0xc3, (byte)0x1c,
                (byte)0x31, (byte)0x41
        };
        new Checksum(SHA256, value);
    }

    @Test
    public void testValidSha512FromString()
    {
        Checksum checksum = new Checksum(SHA512, "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6");

        assertThat(checksum.getValue(), equalTo("07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));
        assertThat(checksum.toString(), equalTo("6:07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));
        assertThat(checksum.getType(), equalTo(SHA512));
    }

    @Test
    public void testValidSha512FromBytes()
    {
        byte[] value = {
                (byte)0x07, (byte)0xe5, (byte)0x47, (byte)0xd9,
                (byte)0x58, (byte)0x6f, (byte)0x6a, (byte)0x73,
                (byte)0xf7, (byte)0x3f, (byte)0xba, (byte)0xc0,
                (byte)0x43, (byte)0x5e, (byte)0xd7, (byte)0x69,
                (byte)0x51, (byte)0x21, (byte)0x8f, (byte)0xb7,
                (byte)0xd0, (byte)0xc8, (byte)0xd7, (byte)0x88,
                (byte)0xa3, (byte)0x09, (byte)0xd7, (byte)0x85,
                (byte)0x43, (byte)0x6b, (byte)0xbb, (byte)0x64,
                (byte)0x2e, (byte)0x93, (byte)0xa2, (byte)0x52,
                (byte)0xa9, (byte)0x54, (byte)0xf2, (byte)0x39,
                (byte)0x12, (byte)0x54, (byte)0x7d, (byte)0x1e,
                (byte)0x8a, (byte)0x3b, (byte)0x5e, (byte)0xd6,
                (byte)0xe1, (byte)0xbf, (byte)0xd7, (byte)0x09,
                (byte)0x78, (byte)0x21, (byte)0x23, (byte)0x3f,
                (byte)0xa0, (byte)0x53, (byte)0x8f, (byte)0x3d,
                (byte)0xb8, (byte)0x54, (byte)0xfe, (byte)0xe6
        };
        Checksum checksum = new Checksum(SHA512, value);

        assertThat(checksum.getValue(), equalTo("07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));
        assertThat(checksum.toString(), equalTo("6:07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"));
        assertThat(checksum.getType(), equalTo(SHA512));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortSha512FromString()
    {
        new Checksum(SHA512, "e7e46ff10df57532f816596327e98f95");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortSha512FromBytes()
    {
        byte[] value = {(byte)0xe7, (byte)0xe4, (byte)0x6f, (byte)0xf1};
        new Checksum(SHA512, value);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongSha512FromString()
    {
        new Checksum(SHA512, "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6000");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongSha512FromBytes()
    {
        byte[] value = {
                (byte)0x07, (byte)0xe5, (byte)0x47, (byte)0xd9,
                (byte)0x58, (byte)0x6f, (byte)0x6a, (byte)0x73,
                (byte)0xf7, (byte)0x3f, (byte)0xba, (byte)0xc0,
                (byte)0x43, (byte)0x5e, (byte)0xd7, (byte)0x69,
                (byte)0x51, (byte)0x21, (byte)0x8f, (byte)0xb7,
                (byte)0xd0, (byte)0xc8, (byte)0xd7, (byte)0x88,
                (byte)0xa3, (byte)0x09, (byte)0xd7, (byte)0x85,
                (byte)0x43, (byte)0x6b, (byte)0xbb, (byte)0x64,
                (byte)0x2e, (byte)0x93, (byte)0xa2, (byte)0x52,
                (byte)0xa9, (byte)0x54, (byte)0xf2, (byte)0x39,
                (byte)0x12, (byte)0x54, (byte)0x7d, (byte)0x1e,
                (byte)0x8a, (byte)0x3b, (byte)0x5e, (byte)0xd6,
                (byte)0xe1, (byte)0xbf, (byte)0xd7, (byte)0x09,
                (byte)0x78, (byte)0x21, (byte)0x23, (byte)0x3f,
                (byte)0xa0, (byte)0x53, (byte)0x8f, (byte)0x3d,
                (byte)0xb8, (byte)0x54, (byte)0xfe, (byte)0xe6,
                (byte)0x31, (byte)0x41
        };
        new Checksum(SHA512, value);
    }

    @Test
    public void testValidAdler32NoPaddingNeeded()
    {
        Checksum checksum = new Checksum(ADLER32, "00675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32FromBytes()
    {
        byte[] value = {0x00, 0x67, 0x5a, (byte)0xf7};

        Checksum checksum = new Checksum(ADLER32, value);

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32NoPaddingNeededUpperCase()
    {
        Checksum checksum = new Checksum(ADLER32, "00675AF7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32PaddingNeeded()
    {
        Checksum checksum = new Checksum(ADLER32, "675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testValidAdler32PaddingNeededByBytes()
    {
        byte[] value = {0x67, 0x5a, (byte)0xf7};

        Checksum checksum = new Checksum(ADLER32, value);

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.toString(), equalTo("1:00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testInvalidAdler32()
    {
        new Checksum(ADLER32, "INVALID");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongAdler32()
    {
        new Checksum(ADLER32, "65e311e678cfc58a1c8c740452b3708");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongAdler32ByBytes()
    {
        byte[] value = {0x00, 0x67, 0x5a, (byte)0xf7, (byte)0xf7};
        new Checksum(ADLER32, value);
    }

    @Test(expected=NullPointerException.class)
    public void testNullStringAdler32()
    {
        new Checksum(ADLER32, (String)null);
    }

    @Test(expected=NullPointerException.class)
    public void testNullByteArrayAdler32()
    {
        new Checksum(ADLER32, (byte[])null);
    }

    @Test
    public void testValidMD5FromString()
    {
        Checksum checksum = new Checksum(MD5_TYPE, "65e311e678cfc58a1c8c740452b3708f");

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.toString(), equalTo("2:65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD5_TYPE));
    }

    @Test
    public void testValidMD5FromBytes()
    {
        byte[] value = {0x65, (byte)0xe3, 0x11, (byte)0xe6, 0x78,
            (byte)0xcf, (byte)0xc5, (byte)0x8a, 0x1c, (byte)0x8c, 0x74, 0x04,
            0x52, (byte)0xb3, 0x70, (byte)0x8f};

        Checksum checksum = new Checksum(MD5_TYPE, value);

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.toString(), equalTo("2:65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD5_TYPE));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortMD5()
    {
        new Checksum(MD5_TYPE, "65e311e678cfc58a1c8c740452b3708");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortMD5ByBytes()
    {
        byte[] value = {0x65, (byte)0xe3, 0x11, (byte)0xe6, 0x78,
            (byte)0xcf, (byte)0xc5, (byte)0x8a, 0x1c, (byte)0x8c, 0x74, 0x04,
            0x52, (byte)0xb3, 0x70};

        new Checksum(MD5_TYPE, value);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongMD5()
    {
        new Checksum(MD5_TYPE, "65e311e678cfc58a1c8c740452b3708ff");
    }


    @Test(expected=IllegalArgumentException.class)
    public void testTooLongMD5ByBytes()
    {
        byte[] value = {0x65, (byte)0xe3, 0x11, (byte)0xe6, 0x78,
            (byte)0xcf, (byte)0xc5, (byte)0x8a, 0x1c, (byte)0x8c, 0x74, 0x04,
            0x52, (byte)0xb3, 0x70, (byte)0x8f, (byte)0x8f};

        new Checksum(MD5_TYPE, value);
    }


    @Test
    public void testValidMD4()
    {
        Checksum checksum = new Checksum(MD4_TYPE, "65e311e678cfc58a1c8c740452b3708f");

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.toString(), equalTo("3:65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD4_TYPE));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooShortMD4()
    {
        new Checksum(MD4_TYPE, "65e311e678cfc58a1c8c740452b3708");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooLongMD4()
    {
        new Checksum(MD4_TYPE, "65e311e678cfc58a1c8c740452b3708ff");
    }

    @Test(expected=NullPointerException.class)
    public void testParseChecksumWithNull()
    {
        Checksum.parseChecksum(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseChecksumWithInvalidString()
    {
        Checksum.parseChecksum("00675af7");
    }

    @Test
    public void testParseChecksumForAdler32NoPaddingNeeded()
    {
        Checksum checksum = Checksum.parseChecksum("1:00675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testParseChecksumForAdler32PaddingNeeded()
    {
        Checksum checksum = Checksum.parseChecksum("1:675af7");

        assertThat(checksum.getValue(), equalTo("00675af7"));
        assertThat(checksum.getType(), equalTo(ADLER32));
    }

    @Test
    public void testParseChecksumForMD5()
    {
        Checksum checksum = Checksum.parseChecksum("2:65e311e678cfc58a1c8c740452b3708f");

        assertThat(checksum.getValue(), equalTo("65e311e678cfc58a1c8c740452b3708f"));
        assertThat(checksum.getType(), equalTo(MD5_TYPE));
   }

    @Test
    public void testParsedZeroPrefixStringEqual()
    {
        Checksum c1 = Checksum.parseChecksum("1:00675af7");
        Checksum c2 = Checksum.parseChecksum("1:675af7");

        assertThat(c1, equalTo(c2));
    }

    @Test
    public void testDifferentValueSameType()
    {
        Checksum c1 = Checksum.parseChecksum("1:00675af7");
        Checksum c2 = Checksum.parseChecksum("1:00675af8");

        assertThat(c1, not(equalTo(c2)));
    }

    @Test
    public void testSameValueDifferType()
    {
        Checksum c1 = Checksum.parseChecksum("2:65e311e678cfc58a1c8c740452b3708f");
        Checksum c2 = Checksum.parseChecksum("3:65e311e678cfc58a1c8c740452b3708f");

        assertThat(c1, not(equalTo(c2)));
    }
}
