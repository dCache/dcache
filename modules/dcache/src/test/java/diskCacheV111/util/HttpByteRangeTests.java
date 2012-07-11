package diskCacheV111.util;

import dmg.util.HttpException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import static org.junit.Assert.*;

public class HttpByteRangeTests{
    private final long LOWER=0;
    private final long UPPER=9999;

    @Test
    public void tryRFC2086Tests() throws HttpException{
        String rangeString;
        List<HttpByteRange> ranges;

         /*The first 500 bytes (byte offsets 0-499, inclusive):*/
        rangeString = "bytes=0-499";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 1);
        assertEquals(ranges.get(0).getLower(), 0);
        assertEquals(ranges.get(0).getUpper(), 499);

        /*The second 500 bytes (byte offsets 500-999, inclusive):*/
        rangeString = "bytes=500-999";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 1);
        assertEquals(ranges.get(0).getLower(), 500);
        assertEquals(ranges.get(0).getUpper(), 999);


        /*The final 500 bytes (byte offsets 9500-9999, inclusive):*/
        rangeString = "bytes=-500";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 1);
        assertEquals(ranges.get(0).getLower(), 9500);
        assertEquals(ranges.get(0).getUpper(), 9999);

        /*Or:*/
        rangeString = "bytes=9500-";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 1);
        assertEquals(ranges.get(0).getLower(), 9500);
        assertEquals(ranges.get(0).getUpper(), 9999);

        /*The first and last bytes only (bytes 0 and 9999):*/
        rangeString = "bytes=0-0,-1";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 2);
        assertEquals(ranges.get(0).getLower(), 0);
        assertEquals(ranges.get(0).getUpper(), 0);
        assertEquals(ranges.get(1).getLower(), 9999);
        assertEquals(ranges.get(1).getUpper(), 9999);

        /*Several legal but non-canonical specifications of the second 500
        bytes (byte offsets 500-999, inclusive):*/
        rangeString = "bytes=500-600,601-999";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 2);
        assertEquals(ranges.get(0).getLower(), 500);
        assertEquals(ranges.get(0).getUpper(), 600);
        assertEquals(ranges.get(1).getLower(), 601);
        assertEquals(ranges.get(1).getUpper(), 999);

        rangeString = "bytes=500-700,601-999";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 2);
        assertEquals(ranges.get(0).getLower(), 500);
        assertEquals(ranges.get(0).getUpper(), 700);
        assertEquals(ranges.get(1).getLower(), 601);
        assertEquals(ranges.get(1).getUpper(), 999);

    }

    @Test
    public void otherTests() throws HttpException{
        String rangeString;
        List<HttpByteRange> ranges;

        rangeString = "bytes=500700, -4d, 601-999  , ,-1000000, ";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        assertEquals(ranges.size(), 2);
        assertEquals(ranges.get(0).getLower(), 601);
        assertEquals(ranges.get(0).getUpper(), 999);
        // Suffix too big; should default to the entire file
        assertEquals(ranges.get(1).getLower(), LOWER);
        assertEquals(ranges.get(1).getUpper(), UPPER);
    }

    // Negative tests

    @Test(expected=HttpException.class)
    public void emtpyRangeTest() throws HttpException{
        String rangeString;
        List<HttpByteRange> ranges;

        rangeString = "bytes=";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        //Expected
    }

    @Test(expected=HttpException.class)
    public void emptySuffixTest() throws HttpException{
        String rangeString;
        List<HttpByteRange> ranges;

        rangeString = "blocks=0-";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
    }

    @Test(expected=HttpException.class)
    public void invalidRangesTest() throws HttpException{
        String rangeString;
        List<HttpByteRange> ranges;

        rangeString = "bytes=500700, -4d, ";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
    }

    @Test
    public void behindUpperLimitTest()
    {
        String rangeString = "bytes=" + (UPPER + 1) + "-" + (UPPER + 2);
        try {
            List<HttpByteRange> ranges = HttpByteRange.parseRanges(rangeString, LOWER, UPPER);
            fail("invalid range not detected");
        }catch (HttpException e) {
            assertEquals(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE, e.getErrorCode());
        }
    }

}

