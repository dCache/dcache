package org.dcache.util;

import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.MD5_TYPE;
import static org.dcache.util.ChecksumType.SHA1;
import static org.dcache.util.ChecksumType.SHA256;
import static org.dcache.util.ChecksumType.SHA512;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.junit.Test;

/**
 * Tests for the ChecksumType
 */
public class ChecksumTypeTests {

    @Test
    public void shouldReturnCorrectBitsForAdler32() {
        assertThat(ADLER32.getBits(), equalTo(32));
    }

    @Test
    public void shouldReturnCorrectNibblesForAdler32() {
        assertThat(ADLER32.getNibbles(), equalTo(8));
    }

    @Test
    public void shouldReturnCorrectBitsForMD5() {
        assertThat(MD5_TYPE.getBits(), equalTo(128));
    }

    @Test
    public void shouldReturnCorrectNibblesForMD5() {
        assertThat(MD5_TYPE.getNibbles(), equalTo(32));
    }

    @Test
    public void shouldReturnCorrectBitsForSHA1() {
        assertThat(SHA1.getBits(), equalTo(160));
    }

    @Test
    public void shouldReturnCorrectNibblesForSHA1() {
        assertThat(SHA1.getNibbles(), equalTo(40));
    }

    @Test
    public void shouldReturnCorrectBitsForSHA256() {
        assertThat(SHA256.getBits(), equalTo(256));
    }

    @Test
    public void shouldReturnCorrectNibblesForSHA256() {
        assertThat(SHA256.getNibbles(), equalTo(64));
    }

    @Test
    public void shouldReturnCorrectBitsForSHA512() {
        assertThat(SHA512.getBits(), equalTo(512));
    }

    @Test
    public void shouldReturnCorrectNibblesForSHA512() {
        assertThat(SHA512.getNibbles(), equalTo(128));
    }

    @Test
    public void shouldReturnSimpleNameOnToString() {
        for (ChecksumType checksumType : ChecksumType.values()) {
            assertEquals(checksumType.getName(), checksumType.toString());
        }
    }

    @Test
    public void shouldCreateAdler32MessageDigest() {
        MessageDigest digest = ADLER32.createMessageDigest();
        digest.update(
              "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));

        assertThat(digest.getAlgorithm(), is(equalTo("ADLER32")));
        assertThat(digest.getDigestLength(), is(equalTo(4)));
        byte[] bytes = {0x5b, (byte) 0xdc, 0x0f, (byte) 0xda};
        assertThat(digest.digest(), is(equalTo(bytes)));
    }

    @Test
    public void shouldCreateMD5MessageDigest() {
        MessageDigest digest = MD5_TYPE.createMessageDigest();
        digest.update(
              "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));

        assertThat(digest.getAlgorithm(), is(equalTo("MD5")));
        assertThat(digest.getDigestLength(), is(equalTo(16)));

        byte[] bytes = {(byte) 0x9e, 0x10, 0x7d, (byte) 0x9d, 0x37, 0x2b, (byte) 0xb6, (byte) 0x82,
              0x6b, (byte) 0xd8, 0x1d, 0x35, 0x42, (byte) 0xa4, 0x19, (byte) 0xd6};
        assertThat(digest.digest(), is(equalTo(bytes)));
    }

    @Test
    public void shouldCreateSHA1MessageDigest() {
        MessageDigest digest = SHA1.createMessageDigest();
        digest.update(
              "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));

        assertThat(digest.getAlgorithm(), is(equalTo("SHA-1")));
        assertThat(digest.getDigestLength(), is(equalTo(20)));

        byte[] bytes = {(byte) 0x2f, (byte) 0xd4, (byte) 0xe1, (byte) 0xc6,
              (byte) 0x7a, (byte) 0x2d, (byte) 0x28, (byte) 0xfc,
              (byte) 0xed, (byte) 0x84, (byte) 0x9e, (byte) 0xe1,
              (byte) 0xbb, (byte) 0x76, (byte) 0xe7, (byte) 0x39,
              (byte) 0x1b, (byte) 0x93, (byte) 0xeb, (byte) 0x12};

        assertThat(digest.digest(), is(equalTo(bytes)));
    }

    @Test
    public void shouldCreateSHA256MessageDigest() {
        MessageDigest digest = SHA256.createMessageDigest();
        digest.update(
              "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));

        assertThat(digest.getAlgorithm(), is(equalTo("SHA-256")));
        assertThat(digest.getDigestLength(), is(equalTo(32)));

        byte[] bytes = {(byte) 0xd7, (byte) 0xa8, (byte) 0xfb, (byte) 0xb3,
              (byte) 0x07, (byte) 0xd7, (byte) 0x80, (byte) 0x94,
              (byte) 0x69, (byte) 0xca, (byte) 0x9a, (byte) 0xbc,
              (byte) 0xb0, (byte) 0x08, (byte) 0x2e, (byte) 0x4f,
              (byte) 0x8d, (byte) 0x56, (byte) 0x51, (byte) 0xe4,
              (byte) 0x6d, (byte) 0x3c, (byte) 0xdb, (byte) 0x76,
              (byte) 0x2d, (byte) 0x02, (byte) 0xd0, (byte) 0xbf,
              (byte) 0x37, (byte) 0xc9, (byte) 0xe5, (byte) 0x92};

        assertThat(digest.digest(), is(equalTo(bytes)));
    }

    @Test
    public void shouldCreateSHA512MessageDigest() {
        MessageDigest digest = SHA512.createMessageDigest();
        digest.update(
              "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));

        assertThat(digest.getAlgorithm(), is(equalTo("SHA-512")));
        assertThat(digest.getDigestLength(), is(equalTo(64)));

        byte[] bytes = {
              (byte) 0x07, (byte) 0xe5, (byte) 0x47, (byte) 0xd9,
              (byte) 0x58, (byte) 0x6f, (byte) 0x6a, (byte) 0x73,
              (byte) 0xf7, (byte) 0x3f, (byte) 0xba, (byte) 0xc0,
              (byte) 0x43, (byte) 0x5e, (byte) 0xd7, (byte) 0x69,
              (byte) 0x51, (byte) 0x21, (byte) 0x8f, (byte) 0xb7,
              (byte) 0xd0, (byte) 0xc8, (byte) 0xd7, (byte) 0x88,
              (byte) 0xa3, (byte) 0x09, (byte) 0xd7, (byte) 0x85,
              (byte) 0x43, (byte) 0x6b, (byte) 0xbb, (byte) 0x64,
              (byte) 0x2e, (byte) 0x93, (byte) 0xa2, (byte) 0x52,
              (byte) 0xa9, (byte) 0x54, (byte) 0xf2, (byte) 0x39,
              (byte) 0x12, (byte) 0x54, (byte) 0x7d, (byte) 0x1e,
              (byte) 0x8a, (byte) 0x3b, (byte) 0x5e, (byte) 0xd6,
              (byte) 0xe1, (byte) 0xbf, (byte) 0xd7, (byte) 0x09,
              (byte) 0x78, (byte) 0x21, (byte) 0x23, (byte) 0x3f,
              (byte) 0xa0, (byte) 0x53, (byte) 0x8f, (byte) 0x3d,
              (byte) 0xb8, (byte) 0x54, (byte) 0xfe, (byte) 0xe6
        };

        assertThat(digest.digest(), is(equalTo(bytes)));
    }

    @Test
    public void shouldCalculateCorrectAdler32Checksum() {
        Checksum checksum = ADLER32.calculate(
              "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));
        assertThat(checksum.getType(), is(ADLER32));
        assertThat(checksum.getValue(), is(equalTo("5bdc0fda")));
    }

    @Test
    public void shouldCalculateCorrectMD5Checksum() {
        Checksum checksum = MD5_TYPE.calculate(
              "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));
        assertThat(checksum.getType(), is(MD5_TYPE));
        assertThat(checksum.getValue(), is(equalTo("9e107d9d372bb6826bd81d3542a419d6")));
    }

    @Test
    public void shouldCalculateCorrectSHA1Checksum() {
        Checksum checksum = SHA1.calculate(
              "The quick brown fox jumps over the lazy dog".getBytes());
        assertThat(checksum.getType(), is(SHA1));
        assertThat(checksum.getValue(), is(equalTo("2fd4e1c67a2d28fced849ee1bb76e7391b93eb12")));
    }

    @Test
    public void shouldCalculateCorrectSHA256Checksum() {
        Checksum checksum = SHA256.calculate(
              "The quick brown fox jumps over the lazy dog".getBytes());
        assertThat(checksum.getType(), is(SHA256));
        assertThat(checksum.getValue(),
              is(equalTo("d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592")));
    }

    @Test
    public void shouldCalculateCorrectSHA512Checksum() {
        Checksum checksum = SHA512.calculate(
              "The quick brown fox jumps over the lazy dog".getBytes());
        assertThat(checksum.getType(), is(SHA512));
        assertThat(checksum.getValue(), is(equalTo(
              "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb642e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6")));
    }
}
