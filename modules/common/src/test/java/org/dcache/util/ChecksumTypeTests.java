package org.dcache.util;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.MD5_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 *  Tests for the ChecksumType
 */
public class ChecksumTypeTests
{
    @Test
    public void shouldReturnCorrectBitsForAdler32()
    {
        assertThat(ADLER32.getBits(), equalTo(32));
    }

    @Test
    public void shouldReturnCorrectNibblesForAdler32()
    {
        assertThat(ADLER32.getNibbles(), equalTo(8));
    }

    @Test
    public void shouldReturnCorrectBitsForMD5()
    {
        assertThat(MD5_TYPE.getBits(), equalTo(128));
    }

    @Test
    public void shouldReturnCorrectNibblesForMD5()
    {
        assertThat(MD5_TYPE.getNibbles(), equalTo(32));
    }

    @Test
    public void shouldReturnSimpleNameOnToString() {
        for(ChecksumType checksumType: ChecksumType.values()) {
            assertEquals(checksumType.getName(), checksumType.toString());
        }
    }

    @Test
    public void shouldCreateAdler32MessageDigest() {
        MessageDigest digest = ADLER32.createMessageDigest();
        digest.update("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));

        assertThat(digest.getAlgorithm(), is(equalTo("ADLER32")));
        assertThat(digest.getDigestLength(), is(equalTo(4)));
        byte[] bytes = {0x5b, (byte)0xdc, 0x0f, (byte)0xda};
        assertThat(digest.digest(), is(equalTo(bytes)));
    }

    @Test
    public void shouldCreateMD5MessageDigest() {
        MessageDigest digest = MD5_TYPE.createMessageDigest();
        digest.update("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));

        assertThat(digest.getAlgorithm(), is(equalTo("MD5")));
        assertThat(digest.getDigestLength(), is(equalTo(16)));
        byte[] bytes =  {(byte)0x9e, 0x10, 0x7d, (byte)0x9d, 0x37, 0x2b, (byte)0xb6, (byte)0x82, 0x6b, (byte)0xd8, 0x1d, 0x35, 0x42, (byte)0xa4, 0x19, (byte)0xd6};
        assertThat(digest.digest(), is(equalTo(bytes)));
    }

    @Test
    public void shouldCalculateCorrectAdler32Checksum() {
        Checksum checksum = ADLER32.calculate("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));
        assertThat(checksum.getType(), is(ADLER32));
        assertThat(checksum.getValue(), is(equalTo("5bdc0fda")));
    }

    @Test
    public void shouldCalculateCorrectMD5Checksum() {
        Checksum checksum = MD5_TYPE.calculate("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8));
        assertThat(checksum.getType(), is(MD5_TYPE));
        assertThat(checksum.getValue(), is(equalTo("9e107d9d372bb6826bd81d3542a419d6")));
    }
}
