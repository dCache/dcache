package org.dcache.commons.util;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * @author timur
 */
public final class Strings {

    private static final Logger LOGGER =
        LoggerFactory.getLogger( Strings.class);

    private static final String[] ZERO_LENGTH_STRING_ARRAY=new String[0];
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

        List<String> matchList = new ArrayList<String>();
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
        return matchList.toArray(ZERO_LENGTH_STRING_ARRAY);
    }
}
