package diskCacheV111.util;

import java.util.List;
import java.text.ParseException;

import org.junit.Test;
import static org.junit.Assert.*;

import diskCacheV111.util.HttpByteRange;


public class HttpByteRangeTests{
    private final long LOWER=0;
    private final long UPPER=9999;

    @Test
    public void tryRFC2086Tests() throws ParseException{
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
    public void otherTests() throws ParseException{
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

    @Test(expected=ParseException.class)
    public void emtpyRangeTest() throws ParseException{
        String rangeString;
        List<HttpByteRange> ranges;

        rangeString = "bytes=";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
        //Expected
    }

    @Test(expected=ParseException.class)
    public void emptySuffixTest() throws ParseException{
        String rangeString;
        List<HttpByteRange> ranges;

        rangeString = "blocks=0-";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
    }

    @Test(expected=ParseException.class)
    public void invalidRangesTest() throws ParseException{
        String rangeString;
        List<HttpByteRange> ranges;

        rangeString = "bytes=500700, -4d, ";
        ranges = HttpByteRange.parseRanges(rangeString,LOWER,UPPER);
    }
}

