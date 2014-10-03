package org.dcache.util;

import java.util.concurrent.TimeUnit;
import org.junit.Test;
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
}
