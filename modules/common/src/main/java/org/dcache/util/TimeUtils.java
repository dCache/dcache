package org.dcache.util;

import com.google.common.collect.ImmutableMap;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.*;


/**
 * Utility classes for dealing with time and durations, mostly with pretty-
 * printing them for human consumption.
 */
public class TimeUtils
{
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd' 'HH:mm:ss.SSS";

    public enum TimeUnitFormat
    {
        /**
         * Display time-units in a short format.
         */
        SHORT,

        /**
         * Display time-units as whole words.
         */
        LONG
    }

    private static final ImmutableMap<TimeUnit,String> SHORT_TIMEUNIT_NAMES =
            ImmutableMap.<TimeUnit,String>builder().
                    put(NANOSECONDS, "ns").
                    put(MICROSECONDS, "\u00B5s"). // U+00B5 is Unicode for microsymbol
                    put(MILLISECONDS, "ms").
                    put(SECONDS, "s").
                    put(MINUTES, "min").
                    put(HOURS, "hours").
                    put(DAYS, "days").
                    build();

    private static final ImmutableMap<TimeUnit,String> LONG_TIMEUNIT_NAMES =
            ImmutableMap.<TimeUnit,String>builder().
                    put(NANOSECONDS, "nanoseconds").
                    put(MICROSECONDS, "microseconds").
                    put(MILLISECONDS, "milliseconds").
                    put(SECONDS, "seconds").
                    put(MINUTES, "minutes").
                    put(HOURS, "hours").
                    put(DAYS, "days").
                    build();

    private TimeUtils()
    {
        // Prevent instantiation.
    }

    public static CharSequence duration(long duration, TimeUnit units, TimeUnitFormat unitFormat)
    {
        return appendDuration(new StringBuilder(), duration, units, unitFormat);
    }


    /**
     * Provide a short, simple human understandable string describing the
     * supplied duration.  The duration is a non-negative value.  The output is
     * appended to the supplied StringBuilder and has the form
     * {@code <number> <space> <units>}, where {@code <number>}
     * is an integer and {@code <units>} is defined by the value of unitFormat.
     */
    public static StringBuilder appendDuration(StringBuilder sb, long duration, TimeUnit units, TimeUnitFormat unitFormat)
    {
        checkArgument(duration >= 0);

        Map<TimeUnit,String> unitsFormat = (unitFormat == TimeUnitFormat.SHORT)
                ? SHORT_TIMEUNIT_NAMES : LONG_TIMEUNIT_NAMES;

        if (units == NANOSECONDS && duration < MICROSECONDS.toNanos(2)) {
            sb.append(units.toNanos(duration)).append(' ').
                    append(unitsFormat.get(NANOSECONDS));
            return sb;
        }

        if (units.toMicros(duration) < MILLISECONDS.toMillis(2) &&
                units.compareTo(MICROSECONDS) <= 0) {
            sb.append(units.toMicros(duration)).append(' ').
                    append(unitsFormat.get(MICROSECONDS));
            return sb;
        }

        long durationInMillis = units.toMillis(duration);

        if (durationInMillis < SECONDS.toMillis(2) &&
                units.compareTo(MILLISECONDS) <= 0) {
            sb.append(durationInMillis).append(' ').
                    append(unitsFormat.get(MILLISECONDS));
        } else if (duration < MINUTES.toMillis(2) &&
                units.compareTo(SECONDS) <= 0) {
            sb.append(units.toSeconds(duration)).append(' ').
                    append(unitsFormat.get(SECONDS));
        } else if (duration < HOURS.toMillis(2) &&
                units.compareTo(MINUTES) <= 0) {
            sb.append(units.toMinutes(duration)).append(' ').
                    append(unitsFormat.get(MINUTES));
        } else if (duration < DAYS.toMillis(2) &&
                units.compareTo(HOURS) <= 0) {
            sb.append(units.toHours(duration)).append(' ').
                    append(unitsFormat.get(HOURS));
        } else {
            sb.append(units.toDays(duration)).append(' ').
                    append(unitsFormat.get(DAYS));
        }

        return sb;
    }


    /**
     * Returns a description of some point in time using some reference point.
     * The appended text is {@code <timestamp> <space> <open-parenth> <integer>
     * <space> <time-unit-word> <space> <relation> <close-parenth>}.  Here are
     * two examples:
     * <pre>
     * 2014-04-20 22:40:32.965 (7 seconds ago)
     * 2014-04-21 02:40:32.965 (3 hours in the future)
     * </pre>
     */
    public static CharSequence relativeTimestamp(long when, long current)
    {
        return appendRelativeTimestamp(new StringBuilder(), when, current);
    }

    /**
     * Append a description of some point in time using some reference point.
     * The appended text is {@code <timestamp> <space> <open-parenth> <integer>
     * <space> <time-unit-word> <space> <relation> <close-parenth>}.  Here are
     * two examples:
     * <pre>
     * 2014-04-20 22:40:32.965 (7 seconds ago)
     * 2014-04-21 02:40:32.965 (3 hours in the future)
     * </pre>
     */
    public static StringBuilder appendRelativeTimestamp(StringBuilder sb,
            long when, long current)
    {
        checkArgument(when > 0);
        checkArgument(current > 0);

        SimpleDateFormat iso8601 = new SimpleDateFormat(TIMESTAMP_FORMAT);
        sb.append(iso8601.format(new Date(when)));

        long diff = Math.abs(when - current);
        sb.append(" (");
        appendDuration(sb, diff, MILLISECONDS, TimeUnitFormat.LONG);
        sb.append(' ');
        sb.append(when < current ? "ago" : "in the future");
        sb.append(')');
        return sb;
    }
}
