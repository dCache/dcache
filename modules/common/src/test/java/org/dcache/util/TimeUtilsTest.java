package org.dcache.util;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.dcache.util.TimeUtils.DurationParser;

import static org.junit.Assert.*;


public class TimeUtilsTest {

    @Test
    public void testDurationOfNanos() {

        CharSequence cs = TimeUtils.duration(13, TimeUnit.NANOSECONDS, TimeUtils.TimeUnitFormat.SHORT);
        assertEquals(cs.toString(), "13 ns");
    }

    @Test
    public void testDurationOfSeconds() {

        CharSequence cs = TimeUtils.duration(13, TimeUnit.SECONDS, TimeUtils.TimeUnitFormat.SHORT);
        assertEquals(cs.toString(), "13 s" );
    }

    @Test
    public void testDurationOfMin() {

        CharSequence cs = TimeUtils.duration(13, TimeUnit.MINUTES, TimeUtils.TimeUnitFormat.SHORT);
        assertEquals(cs.toString(), "13 min");
    }

    @Test
    public void testDurationOfHours() {

        CharSequence cs = TimeUtils.duration(13, TimeUnit.HOURS, TimeUtils.TimeUnitFormat.SHORT);
        assertEquals(cs.toString(), "13 hours");
    }

    @Test
    public void testDurationOfDays() {

        CharSequence cs = TimeUtils.duration(13, TimeUnit.DAYS, TimeUtils.TimeUnitFormat.SHORT);
        assertEquals(cs.toString(), "13 days");
    }

    @Test
    public void testDurationFromNanos() {
        long duration = TimeUnit.SECONDS.toNanos(13);
        CharSequence cs = TimeUtils.duration(duration, TimeUnit.NANOSECONDS, TimeUtils.TimeUnitFormat.SHORT);
        assertEquals(cs.toString(), "13 s");
    }

    @Test
    public void testDurationFromNanosToDays() {
        long duration = TimeUnit.DAYS.toNanos(13);
        CharSequence cs = TimeUtils.duration(duration, TimeUnit.NANOSECONDS, TimeUtils.TimeUnitFormat.SHORT);
        assertEquals(cs.toString(), "13 days");
    }

    @Test
    public void testFormattingFromDaysNoLeadingZeros() {
        String formatted = getFormattedDuration("%D %H:%m:%s.%N");
        assertEquals(formatted, "9 19:3:57.55100");
    }

    @Test
    public void testFormattingFromDays() {
        String formatted = getFormattedDuration("%D %HH:%mm:%ss.%N");
        assertEquals(formatted, "9 19:03:57.55100");
    }

    @Test
    public void testFormattingFromHours() {
        String formatted = getFormattedDuration("%HH:%mm:%ss.%N");
        assertEquals(formatted, "235:03:57.55100");
    }

    @Test
    public void testFormattingFromMinutes() {
        String formatted = getFormattedDuration("%mm minutes, %ss.%N seconds");
        assertEquals(formatted, "14103 minutes, 57.55100 seconds");
    }

    @Test
    public void testFormattingFromSeconds() {
        String formatted = getFormattedDuration("%ss.%N seconds");
        assertEquals(formatted, "846237.55100 seconds");
    }

    @Test
    public void testScrambledFormatting() {
        String formatted = getFormattedDuration("%HH hours, %D days, %s.%N seconds, %m minutes");
        assertEquals(formatted, "19 hours, 9 days, 57.55100 seconds, 3 minutes");
    }

    @Test
    public void testRepeatedFormatting() {
        DurationParser parser = new DurationParser(846237551L, TimeUnit.MILLISECONDS);
        parser.parse(TimeUnit.HOURS);
        parser.parse(TimeUnit.SECONDS);
        assertEquals(parser.get(TimeUnit.HOURS), 235);
        assertEquals(parser.get(TimeUnit.SECONDS), 237);
        parser.clear();
        parser.parseAll();
        assertEquals(parser.get(TimeUnit.HOURS), 19);
    }

    public void testStrictlyDecreasing() {
        DurationParser parser = new DurationParser(846237551L, TimeUnit.MILLISECONDS);
        parser.parse(TimeUnit.HOURS);
        parser.parse(TimeUnit.SECONDS);
        IllegalStateException exception = null;
        try {
            parser.parse(TimeUnit.MINUTES);
        } catch (IllegalStateException e) {
            exception = e;
        }
        assertNotNull(exception);
    }

    @Test
    public void testFormattingWithGaps() {
        String formatted = getFormattedDuration("%H hours, %s.%N seconds");
        assertEquals(formatted, "235 hours, 237.55100 seconds");
    }

    private String getFormattedDuration(String format) {
        long duration = 846237551L;
        TimeUnit durationUnit = TimeUnit.MILLISECONDS;
        return TimeUtils.getFormattedDuration(duration, durationUnit, format);
    }
}
