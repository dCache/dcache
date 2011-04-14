/**
 * Project: dCache-hg
 * Package: org.dcache.gplazma.plugins
 *
 * created on Dec 6, 2010 by karsten
 */
package org.dcache.gplazma.plugins;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ArgumentMapper provides a single static method to create a Map of key value pairs from
 * an argument list in form of an string array.
 * @author karsten
 */
class ArgumentMapFactory {

    private static final Pattern KEY_VALUE_PAIR_PATTERN = Pattern.compile("\"?(\\w[^=]*)=([^\"]*)\"?");
    private static final int KEY_GROUP = 1;
    private static final int VALUE_GROUP = 2;

    /**
     * @param keyset set of valid and not optional keys
     * @param args actual arguments
     * @return Map<String, String> containing key value pairs
     * @throws IllegalArgumentException
     * @throws IndexOutOfBoundsException
     */
    public static Map<String, String> create(Set<String> keyset, String[] args) throws IllegalArgumentException, IndexOutOfBoundsException {

        if ((args.length != keyset.size()*2))
            throw new IllegalArgumentException(String.format("Size of args '%s' does not match key count '%d'. Cannot create ArgumentMap.", Arrays.deepToString(args), keyset.size()));

        Map<String, String> argMap = new HashMap<String, String>();

        List<String> arglist = Arrays.asList(args);
        for (String keyname : keyset) {
            int index = arglist.indexOf(keyname);

            if (index == -1)
                throw new IllegalArgumentException(String.format("Key '%s' does not exists in '%s'.", keyname, Arrays.deepToString(args)));

            if (index % 2 != 0)
                throw new IllegalArgumentException(String.format("Odd index for key '%s' in '%s'.", keyname, Arrays.deepToString(args)));

            if (index >= args.length)
                throw new IllegalArgumentException(String.format("Missing value for key '%s' in '%s'.", keyname, Arrays.deepToString(args)));

            String value = arglist.get(index+1);
            argMap.put(keyname, value);
        }
        return argMap;
    }

    /**
     * @param args contains key value pairs in the form \s*"?(\w[^=\s]*)\s*=([^"]*)"?, trailing whitespaces are trimmed.
     * @return Map<String, String> containing key value pairs
     * @throws IllegalArgumentException if args contains an invalid formed argument list
     */
    public static Map<String, String> createFromKeyValuePairs(String[] args) throws IllegalArgumentException {

        if (args==null) throw new IllegalArgumentException("Args must not be NULL.");

        Map<String, String> argMap = new HashMap<String, String>();

        for (String arg : args) {
            Matcher matcher = KEY_VALUE_PAIR_PATTERN.matcher(arg);
            if (!matcher.matches())
                throw new IllegalArgumentException(String.format("Syntax error in argument list '%s' near token '%s'.", Arrays.deepToString(args), arg));

            argMap.put(matcher.group(KEY_GROUP).trim(), matcher.group(VALUE_GROUP).trim());
        }

        return argMap;
    }

    /**
     * @param keyset contains all expected keys.
     * @param args contains key value pairs in the form "?(\w[^=]*)=([^"]*)"?, trailing whitespaces are trimmed.
     * @return Map<String, String> containing key value pairs
     * @throws IllegalArgumentException if args contains an invalid formed argument list
     */
    public static Map<String, String> createFromAllKeyValuePairs(Set<String> keyset, String[] args) throws IllegalArgumentException {

        if (args==null) throw new IllegalArgumentException("Args must not be NULL.");

        Set<String> keys = new HashSet<String>(keyset);

        Map<String, String> argMap = new HashMap<String, String>();

        for (String arg : args) {
            Matcher matcher = KEY_VALUE_PAIR_PATTERN.matcher(arg);
            if (!matcher.matches())
                throw new IllegalArgumentException(String.format("Syntax error in argument list '%s' near token '%s'.", Arrays.deepToString(args), arg));

            String key = matcher.group(KEY_GROUP).trim();
            String value = matcher.group(VALUE_GROUP).trim();

            if (!keys.contains(key))
                throw new IllegalArgumentException(String.format("Unexpexted key '%s' with value '%s' found in argument list '%s'.", key, value, Arrays.deepToString(args)));

            argMap.put(key, value);
            keys.remove(key);
        }

        if (!keys.isEmpty())
            throw new IllegalArgumentException(String.format("Missing key(s) '%s' in argument list '%s'.", Arrays.deepToString(keyset.toArray()), Arrays.deepToString(args)));

        return argMap;
    }

    public static String getValue(Map<String,String> map, String key, String defaultValue)
    {
        String value = map.get(key);
        return (value == null) ? defaultValue : value;
    }
}
