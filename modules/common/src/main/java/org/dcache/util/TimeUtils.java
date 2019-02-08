package org.dcache.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.*;
import static org.dcache.util.Strings.toThreeSigFig;

/**
 * Utility classes for dealing with time and durations, mostly with pretty-
 * printing them for human consumption.
 */
public class TimeUtils
{
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd' 'HH:mm:ss.SSS";

    /**
     * <p>Compares time units such that the larger unit is
     *      ordered before the smaller.</p>
     */
    public static class DecreasingTimeUnitComparator
                    implements Comparator<TimeUnit>
    {
        @Override
        public int compare(TimeUnit unit1, TimeUnit unit2) {
            if (unit1 == null) {
                return -1;
            }

            if (unit2 == null) {
                return 1;
            }

            long nanos1 = unit1.toNanos(1);
            long nanos2 = unit2.toNanos(1);

            if (nanos1 == nanos2) {
                return 0;
            }

            if (nanos1 < nanos2) {
                return 1;
            }

            return -1;
        }
    }

    /**
     * <p>Parses a duration of a given dimension into other dimensions.</p>
     *
     * <p>The parsed out dimensions are held in an internal map.</p>
     *
     * <p>There can be gaps between the various time dimensions, but it
     *    is understood that successive calls to parse should be
     *    strictly decreasing. Calls to compute a larger unit than
     *    the current unit will throw an exception.</p>
     *
     * <p>The dimensions can be recomputed by calling clear(), but
     *    the original duration given to the parser is fixed.</p>
     */
    public static class DurationParser
    {
        private final Map<TimeUnit, Long> durations;
        private final Long duration;
        private final TimeUnit durationUnit;

        private TimeUnit current;
        private long remainder;

        public DurationParser(Long duration, TimeUnit durationUnit) {
            this.duration = Preconditions.checkNotNull(duration,
                            "duration was null");
            this.durationUnit = Preconditions.checkNotNull(durationUnit,
                            "durationUnit was null");
            durations = new HashMap<>();
            remainder = durationUnit.toNanos(duration);
        }

        public DurationParser parseAll() {
            parse(TimeUnit.DAYS);
            parse(TimeUnit.HOURS);
            parse(TimeUnit.MINUTES);
            parse(TimeUnit.SECONDS);
            parse(TimeUnit.MILLISECONDS);
            parse(TimeUnit.MICROSECONDS);
            parse(TimeUnit.NANOSECONDS);
            return this;
        }

        public void parse(TimeUnit unit) throws IllegalStateException {
            checkStrictlyDecreasing(unit);
            long durationForUnit = unit.convert(remainder, TimeUnit.NANOSECONDS);
            long durationInNanos = unit.toNanos(durationForUnit);
            remainder = remainder - durationInNanos;
            durations.put(unit, durationForUnit);
            current = unit;
        }

        public void clear() {
            durations.clear();
            current = null;
            remainder = durationUnit.toNanos(duration);
        }

        public long get(TimeUnit unit) {
            Long duration = durations.get(unit);
            return duration == null ? 0L: duration;
        }

        private void checkStrictlyDecreasing(TimeUnit next)
                        throws IllegalStateException {
            /*
             * Because this is a decreasing order comparator,
             * it returns -1 when the first element is larger than the
             * second.
             */
            if (comparator.compare(next, current) <= 0) {
                throw new IllegalStateException(next + " is not strictly "
                                + "smaller than " + current);
            }
        }
    }

    /**
     * <p>Returns a given duration broken down into constituent units of all
     *    dimensions and formatted according to the duration format string.</p>
     *
     * <p>The format markers are:</p>
     *
     * <table>
     *     <tr><td>%D</td><td>days</td></tr>
     *     <tr><td>%H</td><td>hours (no leading zero)</td></tr>
     *     <tr><td>%HH</td><td>hours with one leading zero</td></tr>
     *     <tr><td>%m</td><td>minutes (no leading zero)</td></tr>
     *     <tr><td>%mm</td><td>minutes with one leading zero</td></tr>
     *     <tr><td>%s</td><td>seconds (no leading zero)</td></tr>
     *     <tr><td>%ss</td><td>seconds with one leading zero</td></tr>
     *     <tr><td>%S</td><td>milliseconds</td></tr>
     *     <tr><td>%N</td><td>milliseconds+nanoseconds</td></tr>
     * </table>
     *
     * <p>Note that '%' is used here as a reserved symbol as in String formatting.
     *    %s, however, has a different meaning.  This formatting string should
     *    not contain any other placeholders than the ones above and cannot
     *    be combined with normal string formatting.</p>
     */
    public static class DurationFormatter
    {
        private final String format;
        private DurationParser durations;

        public DurationFormatter(String format)
        {
            Preconditions.checkNotNull(format,
                            "Format string must be specified.");
            this.format = format;
        }

        public String format(long duration, TimeUnit unit)
        {
            Preconditions.checkNotNull(unit,
                            "Duration time unit must be specified.");
            TimeUnit[] sortedDimensions = getSortedDimensions();
            durations = new DurationParser(duration, unit);
            for (TimeUnit dimension: sortedDimensions) {
                durations.parse(dimension);
            }
            StringBuilder builder = new StringBuilder();
            replace(builder);
            return builder.toString();
        }

        private TimeUnit[] getSortedDimensions() {
            Set<TimeUnit> units = new HashSet<>();
            char[] sequence = format.toCharArray();
            for (int c = 0; c < sequence.length; c++) {
                switch (sequence[c]) {
                    case '%':
                        ++c;
                        switch (sequence[c]) {
                            case 'D':
                                units.add(TimeUnit.DAYS);
                                break;
                            case 'H':
                                units.add(TimeUnit.HOURS);
                                break;
                            case 'm':
                                units.add(TimeUnit.MINUTES);
                                break;
                            case 's':
                                units.add(TimeUnit.SECONDS);
                                break;
                            case 'S':
                                units.add(TimeUnit.MILLISECONDS);
                                break;
                            case 'N':
                                units.add(TimeUnit.MILLISECONDS);
                                units.add(TimeUnit.MICROSECONDS);
                                units.add(TimeUnit.NANOSECONDS);
                                break;
                            default:
                                throw new IllegalArgumentException(
                                                "No such formatting symbol " + c);
                        }
                        break;
                    default:
                }
            }

            TimeUnit[] sorted = units.toArray(new TimeUnit[units.size()]);
            Arrays.sort(sorted, comparator);
            return sorted;
        }

        private void replace(StringBuilder builder) {
            char[] sequence = format.toCharArray();
            for (int c = 0; c < sequence.length; c++) {
                switch (sequence[c]) {
                    case '%':
                        c = handlePlaceholder(++c,
                                        sequence,
                                        builder);
                        break;
                    default:
                        builder.append(sequence[c]);
                }
            }
        }

        private int handlePlaceholder(int c, char[] sequence, StringBuilder builder)
        {
            switch (sequence[c]) {
                case 'D':
                    builder.append(durations.get(TimeUnit.DAYS));
                    break;
                case 'H':
                    if (sequence[c+1] == 'H') {
                        ++c;
                        builder.append(leadingZero(durations.get(TimeUnit.HOURS)));
                    } else {
                        builder.append(durations.get(TimeUnit.HOURS));
                    }
                    break;
                case 'm':
                    if (sequence[c+1] == 'm') {
                        ++c;
                        builder.append(leadingZero(durations.get(TimeUnit.MINUTES)));
                    } else {
                        builder.append(durations.get(TimeUnit.MINUTES));
                    }
                    break;
                case 's':
                    if (sequence[c+1] == 's') {
                        ++c;
                        builder.append(leadingZero(durations.get(TimeUnit.SECONDS)));
                    } else {
                        builder.append(durations.get(TimeUnit.SECONDS));
                    }
                    break;
                case 'S':
                    builder.append(durations.get(TimeUnit.MILLISECONDS));
                    break;
                case 'N':
                    builder.append(durations.get(TimeUnit.MILLISECONDS))
                           .append(durations.get(TimeUnit.MICROSECONDS))
                           .append(durations.get(TimeUnit.NANOSECONDS));
                    break;
                default:
                    throw new IllegalArgumentException
                                    ("No such formatting symbol " + c);
            }

            return c;
        }

        private String leadingZero(Long value) {
            String valueString = String.valueOf(value);
            if (valueString.length() < 2) {
                return '0' + valueString;
            }
            return valueString;
        }
    }

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

    private static final DecreasingTimeUnitComparator comparator
                    = new DecreasingTimeUnitComparator();

    private TimeUtils()
    {
        // Prevent instantiation.
    }

    public static CharSequence duration(long duration, TimeUnit units, TimeUnitFormat unitFormat)
    {
        return appendDuration(new StringBuilder(), duration, units, unitFormat);
    }

    /**
     * @see DurationFormatter
     *
     * @param duration to be expressed
     * @param unit of the duration
     * @param format as specified above.
     * @return formatted string
     */
    public static String getFormattedDuration(long duration,
                                              TimeUnit unit,
                                              String format)
    {
        return new DurationFormatter(format).format(duration, unit);
    }

    /**
     * Returns short sting form for given {@ link TimeUnit}.
     */
    public static String unitStringOf(TimeUnit unit) {
        return SHORT_TIMEUNIT_NAMES.get(unit);
    }

    /**
     * Provide a short, simple human understandable string describing the
     * supplied duration.  The duration is a non-negative value.  The output is
     * appended to the supplied StringBuilder and has the form
     * {@code <number> <space> <units>}, where {@code <number>}
     * is an integer and {@code <units>} is defined by the value of unitFormat.
     */
    public static StringBuilder appendDuration(StringBuilder sb, long duration,
                    TimeUnit units, TimeUnitFormat unitFormat)
    {
        checkArgument(duration >= 0);

        Map<TimeUnit,String> unitsFormat = (unitFormat == TimeUnitFormat.SHORT)
                ? SHORT_TIMEUNIT_NAMES : LONG_TIMEUNIT_NAMES;

        TimeUnit targetUnit = displayUnitFor(duration, units);
        return sb.append(targetUnit.convert(duration, units)).append(' ').
                    append(unitsFormat.get(targetUnit));
    }


    public static TimeUnit displayUnitFor(long duration, TimeUnit units)
    {
        if (units == NANOSECONDS && duration < MICROSECONDS.toNanos(2)) {
            return NANOSECONDS;
        }

        if (units.toMicros(duration) < MILLISECONDS.toMicros(2) &&
                units.compareTo(MICROSECONDS) <= 0) {
            return MICROSECONDS;
        }

        long durationInMillis = units.toMillis(duration);

        if (durationInMillis < SECONDS.toMillis(2) &&
                units.compareTo(MILLISECONDS) <= 0) {
            return MILLISECONDS;
        } else if (durationInMillis < MINUTES.toMillis(2) &&
                units.compareTo(SECONDS) <= 0) {
            return SECONDS;
        } else if (durationInMillis < HOURS.toMillis(2) &&
                units.compareTo(MINUTES) <= 0) {
            return MINUTES;
        } else if (durationInMillis < DAYS.toMillis(2) &&
                units.compareTo(HOURS) <= 0) {
            return HOURS;
        } else {
            return DAYS;
        }
    }

    private static double seconds(Duration duration)
    {
        return duration.getSeconds() + duration.getNano() / 1_000_000_000.0;
    }

    private static String convert(Duration duration, ChronoUnit unit, String name)
    {
        double value = seconds(duration) / seconds(unit.getDuration());
        return toThreeSigFig(value, 2000) + " " + name;
    }

    private static Optional<String> inUnits(Duration duration, ChronoUnit unit, String name)
    {
        Duration cutoff = unit.getDuration().multipliedBy(2);
        if (duration.compareTo(cutoff) >= 0) {
            return Optional.of(convert(duration, unit, name));
        } else {
            return Optional.empty();
        }
    }

    public static Optional<String> describe(Duration duration)
    {
        if (duration.isZero()) {
            return Optional.empty();
        }

        Optional<String> value = inUnits(duration, ChronoUnit.DAYS, "days");
        if (!value.isPresent()) {
            value = inUnits(duration, ChronoUnit.HOURS, "hours");
        }
        if (!value.isPresent()) {
            value = inUnits(duration, ChronoUnit.MINUTES, "minutes");
        }
        if (!value.isPresent()) {
            value = inUnits(duration, ChronoUnit.SECONDS, "s");
        }
        if (!value.isPresent()) {
            value = inUnits(duration, ChronoUnit.MILLIS, "ms");
        }
        if (!value.isPresent()) {
            value = inUnits(duration, ChronoUnit.MICROS, "\u00B5s");
        }
        if (!value.isPresent()) {
            value = Optional.of(convert(duration, ChronoUnit.NANOS, "ns"));
        }
        return value;
    }

    public static String describeDuration(double duration, TimeUnit units)
    {
        TimeUnit targetUnits = displayUnitFor(Math.round(duration), units);
        double scaledDuration = convert(duration, units, targetUnits);
        return toThreeSigFig(scaledDuration, 2000) + " " + SHORT_TIMEUNIT_NAMES.get(targetUnits);
    }

    public static String describeDuration(StatisticalSummary duration, TimeUnit units)
    {
        double min = duration.getMin();
        double max = duration.getMax();

        if (min == max) {
            return describeDuration(max, units);
        } else {
            double mean = duration.getMean();
            double sem = duration.getStandardDeviation() / Math.sqrt(duration.getN());
            String meanDescription;
            if (sem == 0) {
                meanDescription = describeDuration(mean, units);
            } else {
                TimeUnit targetUnits = displayUnitFor(Math.round(mean), units);
                double scaledMean = convert(mean, units, targetUnits);
                double scaledSem = convert(sem, units, targetUnits);

                meanDescription = "(" + toThreeSigFig(scaledMean, 2000, scaledSem) + ") "
                        + SHORT_TIMEUNIT_NAMES.get(targetUnits);
            }

            double sd = duration.getStandardDeviation();

            return " min. " + describeDuration(min, units)
                        + ", mean " + meanDescription
                        + ", SD " + describeDuration(sd, units)
                        + ", max. " + describeDuration(max, units);
        }
    }

    private static double convert(double source, TimeUnit sourceUnits, TimeUnit targetUnits)
    {
        return source / sourceUnits.convert(1, targetUnits);
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

    public static CharSequence relativeTimestamp(Instant when)
    {
        return appendRelativeTimestamp(new StringBuilder(), when.toEpochMilli(),
                System.currentTimeMillis(), TimeUnitFormat.SHORT);
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
        return appendRelativeTimestamp(sb, when, current, TimeUnitFormat.LONG);
    }

    public static StringBuilder appendRelativeTimestamp(StringBuilder sb,
            long when, long current, TimeUnitFormat format)
    {
        checkArgument(when > 0);
        checkArgument(current > 0);

        SimpleDateFormat iso8601 = new SimpleDateFormat(TIMESTAMP_FORMAT);
        sb.append(iso8601.format(new Date(when)));

        long diff = Math.abs(when - current);
        sb.append(" (");
        appendDuration(sb, diff, MILLISECONDS, format);
        sb.append(' ');
        sb.append(when < current ? "ago" : "in the future");
        sb.append(')');
        return sb;
    }

    public static long getMillis(Properties properties, String key)
    {
        return TimeUnit.valueOf(properties.getProperty(key + ".unit")).toMillis(
                Long.parseLong(properties.getProperty(key)));
    }

    /**
     * Convert a TimeUnit to the equivalent ChronoUnit.
     * REVISIT: this functionality is available from Java 9 onwards as
     * {@code TimeUnit.toChronoUnit()}.
     */
    public static ChronoUnit toChronoUnit(TimeUnit unit)
    {
        switch (unit) {
        case DAYS:
            return ChronoUnit.DAYS;
        case HOURS:
            return ChronoUnit.HOURS;
        case MINUTES:
            return ChronoUnit.MINUTES;
        case SECONDS:
            return ChronoUnit.SECONDS;
        case MILLISECONDS:
            return ChronoUnit.MILLIS;
        case MICROSECONDS:
            return ChronoUnit.MICROS;
        case NANOSECONDS:
            return ChronoUnit.NANOS;
        }
        throw new IllegalArgumentException("Unknown TimeUnit: " + unit);
    }

    public static Duration durationOf(long value, TimeUnit unit)
    {
        return Duration.of(value, toChronoUnit(unit));
    }
}
