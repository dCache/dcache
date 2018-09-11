package org.dcache.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.net.InetAddresses;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.util.ByteUnit.Type.BINARY;
import static org.dcache.util.ByteUnits.isoSymbol;

/**
 *
 * @author timur
 */
public final class Strings {

    private static final Logger LOGGER =
        LoggerFactory.getLogger( Strings.class);

    private static final String ANSI_ESCAPE = "\u001b[";
    private static final String[] ZERO_LENGTH_STRING_ARRAY=new String[0];
    private static final String INFINITY = "infinity";
    private static final DecimalFormat THREE_SIG_FIG_FORMAT = new DecimalFormat("0.##E0");

    static {
        DecimalFormatSymbols symbols = THREE_SIG_FIG_FORMAT.getDecimalFormatSymbols();
        symbols.setExponentSeparator("x10^");
        symbols.setNaN("-");
        THREE_SIG_FIG_FORMAT.setDecimalFormatSymbols(symbols);
    }


    /**
     * Splits a string into an array of strings using white space as dividers
     * Substring surrounded by white space and single or double quotes is
     * treated as a single indivisible string, and white space inside such
     * substring is not used as a divider.
     * So the following string
     * <code> arg1 arg2 "this is an argument 3" 'arg 4'</code>
     * will be split into String array
     * <code> {"arg1","arg2","this is an argument 3", "arg 4"} </code>
     * Quotes embedded into the strings of non white spaces
     * (i.e. <code> aaa"bbb </code> or <code> ccc"ddd eee"fff </code> )
     * are not supported at this time and the behavior is undefined.
     * @param argumentString
     * @return String array, a result of argument string split,
     * zero length array of strings if the argument string is null
     */
    public static String[] splitArgumentString(String argumentString) {
        LOGGER.debug("splitting argument string {}",argumentString);
        if(argumentString == null) {
            return ZERO_LENGTH_STRING_ARRAY;
        }
        argumentString = argumentString.trim();
        Pattern regex = Pattern.compile(
            "\"([^\"]*)\""+    // first group matches string surronded
                               // by double quotes
            "|'([^']*)'"+      // second group is for strings in single
                               // quotes
            "|([^\\s]+)");     // last group matches everything else
                               // without the spaces
        Matcher regexMatcher = regex.matcher(argumentString);

        List<String> matchList = new ArrayList<>();
        while(regexMatcher.find()) {
         if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                String groupMatch=  regexMatcher.group(1);
                LOGGER.debug("first group matched [{}]",groupMatch);
                matchList.add(groupMatch);
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                String groupMatch=  regexMatcher.group(2);
                LOGGER.debug("second group matched [{}]",groupMatch);
                matchList.add(groupMatch);
            } else if (regexMatcher.group(3) != null) {
                //everything else
                String groupMatch=  regexMatcher.group(3);
                LOGGER.debug("third group matched [{}]",groupMatch);
                matchList.add(groupMatch);
            }
        }
        return matchList.toArray(new String[matchList.size()]);
    }

    public static int plainLength(String s)
    {
        int length = s.length();
        int plainLength = length;
        int i = s.indexOf(ANSI_ESCAPE);
        while (i > -1) {
            plainLength -= ANSI_ESCAPE.length();
            i += ANSI_ESCAPE.length();
            if (i < length) {
                while (i + 1 < length && (s.charAt(i) < 64 || s.charAt(i) > 126)) {
                    i++;
                    plainLength--;
                }
                i++;
                plainLength--;
            }
            i = s.indexOf(ANSI_ESCAPE, i);
        }
        return plainLength;
    }

    /**
     * Locates the last occurrence of a white space character after fromIndex and before
     * wrapLength characters, or the first occurrence of a white space character after
     * fromIndex if there is no white space before wrapLength characters.
     *
     * ANSI escape sequences are considered to have zero width.
     */
    private static int indexOfNextWrap(char[] chars, int fromIndex, int wrapLength)
    {
        int lastWrap = -1;
        int max = fromIndex + wrapLength;
        int length = chars.length;
        for (int i = fromIndex; i < length && (i <= max || lastWrap == -1); i++) {
            if (Character.isWhitespace(chars[i])) {
                lastWrap = i;
            } else if (chars[i] == 27 && i < length && chars[i + 1] == '[') {
                i += 2;
                max += 2;
                for (;i < length && (chars[i] < 64 || chars[i] > 126); i++) {
                    max++;
                }
                max++;
            }
        }
        return length <= max || lastWrap == -1 ? length : lastWrap;
    }

    /**
     * Wraps a text to a particular width. Leading white space is
     * repeated in front of every wrapped line.
     *
     * ANSI escape sequences are considered to have zero width.
     *
     * @param indent String to place at the beginning of each line
     * @param str String to wrap
     * @param wrapLength Width to wrap to excluding indent
     * @return Wrapped string.
     */
    public static String wrap(String indent, String str, int wrapLength)
    {
        int offset = 0;
        StringBuilder out = new StringBuilder(str.length());

        char[] chars = str.toCharArray();
        int length = chars.length;
        while (offset < length) {
            int bop = offset;        // beginning of paragraph
            while (offset < length && chars[offset] == ' ') {
                offset++;
            }
            int pil = offset - bop;  // paragraph indentation length

            int eop = offset;        // end of paragraph
            while (eop < length && chars[eop] != '\n') {
                eop++;
            }

            int spaceToWrapAt;
            while ((spaceToWrapAt = indexOfNextWrap(chars, offset, wrapLength - indent.length() - pil)) < eop) {
                out.append(indent);
                out.append(chars, bop, pil);
                out.append(chars, offset, spaceToWrapAt - offset);
                out.append('\n');
                offset = spaceToWrapAt + 1;

                // Skip leading spaces on next line
                while (offset < length && chars[offset] == ' ') {
                    offset++;
                }
            }

            out.append(indent).append(chars, bop, pil).append(chars, offset, eop - offset).append('\n');
            offset = eop + 1;
        }

        return out.toString();
    }

    /**
     * Convert a {@link Method} to a String signature. The provided {@link Character}
     * {@code c} used as a delimiter in the resulting string.
     * @param m method to get signature from
     * @param c delimiter to use
     * @return method's signature as a String
     */
    public static String toStringSignature(Method m, Character c) {
        StringBuilder sb = new StringBuilder();
        sb.append(m.getName());
        sb.append('(');
        Joiner.on(c).appendTo(sb, transform(asList(m.getParameterTypes()), Class::getSimpleName));
        sb.append(')');
        return sb.toString();
    }

    /**
     * Like Integer#parseInt, but parses "infinity" to Integer.MAX_VALUE.
     */
    public static int parseInt(String s)
            throws NumberFormatException
    {
        return s.equals(INFINITY) ? Integer.MAX_VALUE : Integer.parseInt(s);
    }

    /**
     * Like Long#parseLong, but parses "infinity" to Long.MAX_VALUE.
     */
    public static long parseLong(String s)
            throws NumberFormatException
    {
        return s.equals(INFINITY) ? Long.MAX_VALUE : Long.parseLong(s);
    }

    /**
     * Parses a string to a time value, converting from milliseconds to the specified unit. Parses
     * "infinity" to Long.MAX_VALUE.
     */
    public static long parseTime(String s, TimeUnit unit)
    {
        return s.equals(INFINITY) ? Long.MAX_VALUE : MILLISECONDS.convert(Long.parseLong(s), unit);
    }

    /**
     * Returns a string representation of the specified object or the empty string
     * for a null argument. In contrast to Object#toString, this method recognizes
     * array arguments and returns a suitable string form.
     */
    public static String toString(Object value)
    {
        if (value == null) {
            return "";
        } else if (value.getClass().isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType == Boolean.TYPE) {
                return Arrays.toString((boolean[]) value);
            } else if (componentType == Byte.TYPE) {
                return Arrays.toString((byte[]) value);
            } else if (componentType == Character.TYPE) {
                return Arrays.toString((char[]) value);
            } else if (componentType == Double.TYPE) {
                return Arrays.toString((double[]) value);
            } else if (componentType == Float.TYPE) {
                return Arrays.toString((float[]) value);
            } else if (componentType == Integer.TYPE) {
                return Arrays.toString((int[]) value);
            } else if (componentType == Long.TYPE) {
                return Arrays.toString((long[]) value);
            } else if (componentType == Short.TYPE) {
                return Arrays.toString((short[]) value);
            } else {
                return Arrays.deepToString((Object[]) value);
            }
        } else {
            return value.toString();
        }
    }

    /**
     * Returns a string representation of the specified object or the empty string
     * for a null argument. In contrast to Object#toString, this method recognizes
     * array arguments and returns a suitable string form. In contrast to Strings#toString,
     * the object arrays are split over multiple lines.
     */
    public static String toMultilineString(Object value)
    {
        if (value == null) {
            return "";
        } else if (value.getClass().isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType == Boolean.TYPE) {
                return Arrays.toString((boolean[]) value);
            } else if (componentType == Byte.TYPE) {
                return Arrays.toString((byte[]) value);
            } else if (componentType == Character.TYPE) {
                return Arrays.toString((char[]) value);
            } else if (componentType == Double.TYPE) {
                return Arrays.toString((double[]) value);
            } else if (componentType == Float.TYPE) {
                return Arrays.toString((float[]) value);
            } else if (componentType == Integer.TYPE) {
                return Arrays.toString((int[]) value);
            } else if (componentType == Long.TYPE) {
                return Arrays.toString((long[]) value);
            } else if (componentType == Short.TYPE) {
                return Arrays.toString((short[]) value);
            } else {
                return Joiner.on('\n').join(transform(asList((Object[]) value), (Function<Object, Object>) Strings::toString));
            }
        } else {
            return value.toString();
        }
    }

    public static Optional<String> combine(Optional<String> first, String seperator, Optional<String> second)
    {
        if (first.isPresent()) {
            if (second.isPresent()) {
                return Optional.of(first.get() + seperator + second.get());
            } else {
                return first;
            }
        } else {
            return second;
        }
    }

    /**
     * Text that is split into lines, each line has a prefix and the resulting
     * lines are combined.  The returned String does NOT end with a new-line
     * character.
     */
    public static String indentLines(String indent, String text)
    {
        return new BufferedReader(new StringReader(text))
                .lines()
                .map(s -> indent + s)
                .collect(Collectors.joining("\n"));
    }

    public static String describeBandwidth(StatisticalSummary bandwidth)
    {
        double min = bandwidth.getMin();
        double max = bandwidth.getMax();
        double sd = bandwidth.getStandardDeviation();

        if (min == max) {
            return describeBandwidth(max);
        } else {
            double mean = bandwidth.getMean();
            double sem = bandwidth.getStandardDeviation() / Math.sqrt(bandwidth.getN());
            String meanDescription;
            if (sem == 0) {
                meanDescription = describeBandwidth(mean);
            } else {
                ByteUnit units = BINARY.unitsOf(mean);
                double scaledMean = units.convert(mean, ByteUnit.BYTES);
                double scaledSem = units.convert(sem, ByteUnit.BYTES);
                meanDescription = "(" + toThreeSigFig(scaledMean, 1024, scaledSem) + ") "
                        + isoSymbol().of(units) + "/s";
            }

            return "min. " + describeBandwidth(min)
                    + ", mean " + meanDescription
                    + ", SD " + describeBandwidth(sd)
                    + ", max. " + describeBandwidth(max);
        }
    }

    public static String describeSize(StatisticalSummary size)
    {
        long min = Math.round(size.getMin());
        long max = Math.round(size.getMax());
        if (min == max) {
            return describeSize(max);
        } else {
            double mean = size.getMean();
            double sem = size.getStandardDeviation() / Math.sqrt(size.getN());
            String meanDescription;
            if (sem == 0) {
                meanDescription = describeSize(Math.round(mean));
            } else {
                ByteUnit units = BINARY.unitsOf(mean);
                double scaledMean = units.convert(mean, ByteUnit.BYTES);
                double scaledSem = units.convert(sem, ByteUnit.BYTES);
                meanDescription = "(" + toThreeSigFig(scaledMean, 1024, scaledSem) + ") " + isoSymbol().of(units);
            }

            long sd = Math.round(size.getStandardDeviation());
            return "min. " + describeSize(min)
                    + ", mean " + meanDescription
                    + ", SD " + describeSize(sd)
                    + ", max. " + describeSize(max);
        }
    }

    public static String describeInteger(StatisticalSummary statistics)
    {
        long min = Math.round(statistics.getMin());
        long max = Math.round(statistics.getMax());

        if (min == max) {
            return String.valueOf(max);
        } else {
            double mean = statistics.getMean();
            double sd = statistics.getStandardDeviation();
            double sem = sd / Math.sqrt(statistics.getN());

            String meanDescription;
            if (sem == 0) {
                meanDescription = String.valueOf(Math.round(mean));
            } else {
                meanDescription = "(" + toThreeSigFig(mean, 1000, sem) + ")";
            }

            return " min. " + min
                        + ", mean " + meanDescription
                        + ", SD " + toThreeSigFig(sd, 1000)
                        + ", max. " + max;
        }
    }

    /**
     * Provide a file/data size (in bytes) a human readable form, using binary units
     * and ISO symbols.  For example, 10 --> "10 B",  2560 --> "2.5 KiB".
     * @param size The size to represent.
     * @return a String describing this value.
     */
    public static String humanReadableSize(long size)
    {
        ByteUnit units = BINARY.unitsOf(size);
        return toThreeSigFig(units.convert((double)size, ByteUnit.BYTES), 1024)
                + " " + isoSymbol().of(units);
    }

    /**
     * Provide a description of a file/data size (in bytes).  The output shows
     * the exact number with units ("B").  If the value is larger than 1 KiB
     * then the scaled value is shown in the most appropriate units.  For
     * example 10 --> "10 B", 2560 --> "2560 B (2.5 KiB)".
     * @param size The size to represent.
     * @return a String describing this value.
     */
    public static String describeSize(long size)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(size).append(' ').append(isoSymbol().of(ByteUnit.BYTES));

        ByteUnit units = BINARY.unitsOf(size);
        if (units != ByteUnit.BYTES) {
            sb.append(" (").append(toThreeSigFig(units.convert((double)size, ByteUnit.BYTES), 1024))
                    .append(' ').append(isoSymbol().of(units)).append(')');
        }

        return sb.toString();
    }

    public static String describeBandwidth(double value)
    {
        ByteUnit units = BINARY.unitsOf(value);
        return toThreeSigFig(units.convert((double)value, ByteUnit.BYTES), 1024)
                + " " + isoSymbol().of(units) + "/s";
    }

    public static String toThreeSigFig(double value, double max)
    {
        if (value == 0) {
            return "0";
        }

        if (value >= 1) {
            if (value < 10) {
                return String.format("%.2f", value);
            } else if (value < 100) {
                return String.format("%.1f", value);
            } else if (value < max) {
                return String.format("%.0f", value);
            }
        }
        return THREE_SIG_FIG_FORMAT.format(value);
    }

    public static String toThreeSigFig(double value, double max, double uncertainty)
    {
        if (uncertainty == 0) {
            return toThreeSigFig(value, max);
        }

        if (value == 0) {
            return "0 ± " + toThreeSigFig(uncertainty, max);
        }

        if (value >= 1) {
            if (value < 10) {
                return String.format("%.2f", value) + " ± " + String.format("%.2f", uncertainty);
            } else if (value < 100) {
                return String.format("%.1f", value) + " ± " + String.format("%.1f", uncertainty);
            } else if (value < max) {
                return String.format("%.0f", value) + " ± " + String.format("%.0f", uncertainty);
            }
        }
        return THREE_SIG_FIG_FORMAT.format(value) + " ± " + THREE_SIG_FIG_FORMAT.format(uncertainty);
    }

    public static String describe(SocketAddress address)
    {
        if (address instanceof InetSocketAddress) {
            InetSocketAddress inet = (InetSocketAddress) address;
            return InetAddresses.toUriString(inet.getAddress()) + ":" + inet.getPort();
        } else {
            return address.toString();
        }
    }

    public static CharSequence describe(Optional<Instant> when)
    {
        return when.map(TimeUtils::relativeTimestamp).orElse("never");
    }
}
