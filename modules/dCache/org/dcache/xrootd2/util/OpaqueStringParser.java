package org.dcache.xrootd2.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OpaqueStringParser {

    public final static char OPAQUE_PREFIX = '&';
    public final static char OPAQUE_SEPARATOR = '=';

    /**
     * The opaque information is included in the path in a format similar to
     * URL-encoding (&key1=val1&key2=val2...). This method translates that
     * encoding to a map, mapping from the keys found in the opaque string to
     * the values found in the opaque string.
     *
     * @param opaque The opaque string, as usually attached to the path
     * @return Map from keys to values in the opaque string
     * @throws ParseException if value is missing for a key in the string
     */
    public static Map<String,String> getOpaqueMap(String opaque)
                                                        throws ParseException
    {
        if (opaque == null) {
            return Collections.emptyMap();
        } else {
            Map<String,String> map = new HashMap<String,String>();
            int tokenStart;
            int tokenEnd = 0;

            while ((tokenStart = opaque.indexOf(OPAQUE_PREFIX, tokenEnd))
                    != -1) {
                tokenEnd = opaque.indexOf(OPAQUE_PREFIX, ++tokenStart);

                if (tokenEnd == -1) {
                    tokenEnd = opaque.length();
                }

                int delimiter = opaque.indexOf(OPAQUE_SEPARATOR,tokenStart);
                if (delimiter == -1 || delimiter >= tokenEnd) {
                    String variable = opaque.substring(tokenStart, tokenEnd);
                    throw new ParseException("Opaque information is missing a"
                                             + "value for variable " +
                                              variable);
                }

                map.put(opaque.substring(tokenStart, delimiter),
                        opaque.substring(delimiter + 1, tokenEnd));
            }

            return map;
        }
    }
}
