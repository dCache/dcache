package org.dcache.commons.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
     * Wraps a text to a particular width.
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
            int eop = offset;
            while (eop < length && chars[eop] != '\n') {
                eop++;
            }

            int spaceToWrapAt;
            while ((spaceToWrapAt = indexOfNextWrap(chars, offset, wrapLength)) < eop) {
                out.append(indent);
                out.append(chars, offset, spaceToWrapAt - offset);
                out.append('\n');
                offset = spaceToWrapAt + 1;

                // Skip leading spaces on next line
                while (offset < length && chars[offset] == ' ') {
                    offset++;
                }
            }

            out.append(indent).append(chars, offset, eop - offset).append('\n');
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
        sb.append(Joiner.on(c).join(transform(asList(m.getParameterTypes()), GET_SIMPLE_NAME)));
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
                return Joiner.on('\n').join(transform(asList((Object[]) value), TO_STRING));
            }
        } else {
            return value.toString();
        }
    }

    public static final Function<Object, Object> TO_STRING = new Function<Object, Object>()
    {
        @Override
        public Object apply(Object input)
        {
            return Strings.toString(input);
        }
    };

    private static final Function<Class<?>, String> GET_SIMPLE_NAME = new Function<Class<?>, String>() {
        @Override
        public String apply(Class<?> c) {
            return c.getSimpleName();
        }
    };
}
