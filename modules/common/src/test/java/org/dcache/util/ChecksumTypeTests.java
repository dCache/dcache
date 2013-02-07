package org.dcache.util;

import org.junit.Test;
import static org.dcache.util.ChecksumType.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;


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
}
