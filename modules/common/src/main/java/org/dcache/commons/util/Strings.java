package org.dcache.commons.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterators.forArray;
import static com.google.common.collect.Iterators.transform;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 * @author timur
 */
public final class Strings {

    private static final Logger LOGGER =
        LoggerFactory.getLogger( Strings.class);

    private static final String[] ZERO_LENGTH_STRING_ARRAY=new String[0];
    private static final String INFINITY = "infinity";

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

    /**
     * Wraps a text to a particular width.
     *
     * @param indent String to place at the beginning of each line
     * @param str String to wrap
     * @param wrapLength Width to wrap to excluding indent
     * @return Wrapped string.
     */
    public static String wrap(String indent, String str, int wrapLength)
    {
        int offset = 0;
        StringBuilder out = new StringBuilder();

        while (offset < str.length()) {
            int eop = str.indexOf('\n', offset);
            if (eop < 0) {
                eop = str.length();
            }

            boolean firstLine = true;
            while ((eop - offset) > wrapLength) {
                if (!firstLine && str.charAt(offset) == ' ') {
                    offset++;
                    continue;
                }

                int spaceToWrapAt = str.lastIndexOf(' ', wrapLength + offset);
                // if the next string with length wrapLength doesn't contain ' '
                if (spaceToWrapAt < offset) {
                    spaceToWrapAt = str.indexOf(' ', wrapLength + offset);
                    // if no more ' '
                    if (spaceToWrapAt < 0 || spaceToWrapAt > eop) {
                        break;
                    }
                }

                out.append(indent);
                out.append(str.substring(offset, spaceToWrapAt));
                out.append("\n");
                offset = spaceToWrapAt + 1;
                firstLine = false;
            }

            out.append(indent).append(str.substring(offset, eop)).append('\n');
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
        sb.append("(");
        sb.append(Joiner.on(c).join(transform(forArray(m.getParameterTypes()), GET_SIMPLE_NAME)));
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

    private static final Function<Class<?>, String> GET_SIMPLE_NAME = new Function<Class<?>, String>() {
        @Override
        public String apply(Class<?> c) {
            return c.getSimpleName();
        }
    };
}
