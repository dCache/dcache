package org.dcache.xrootd2.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * According to the xrootd specification, an opaque string has the following
 * format
 *
 * opaque (Xrdv296 SPEC Section 3.13.1):
 *     "Opaque information is passed by suffixing the path with a
 *     question mark (?) and then coding the opaque information as a series of
 *     ampersand prefixed (&) variable names immediately followed by an
 *     equal sign (=) prefix value."

 *
 * token (follows the SPEC in Feichtinger, Peters: "Authorization of Data
 *        Access in Distributed Systems", Section IIIA., page 3):
 *      "The authorization envelope which is obtained from the catalogue
 *      service is appended to an URL as opaque information following the
 *      syntax:
 *      URL : root : // < host -ip >:< port > // < file - path >?authz =
 *      < acess - envelope > & vo = < vo -name >
 *
 * In summary, this yields the following format for opaque information and
 * token:
 *
 * ?&opaqueKey1=value1&opaqueKey2=value2&opaqueKey3=value3?authz=sectoken&vo=
 *  voname
 *
 * Experience shows that the first value after the question mark is not
 * always ampersand-prefixed, despite the protocol specification.
 *
 * We have reported the following bug to the root developers:
 *
 *  https://savannah.cern.ch/bugs/?75478
 *
 * @author tzangerl
 *
 */
public class OpaqueStringParser {
    public final static char OPAQUE_STRING_PREFIX = '?';
    public final static char OPAQUE_PREFIX = '&';
    public final static char OPAQUE_SEPARATOR = '=';

    /**
     * The opaque information is included in the path in a format similar to
     * URL-encoding (&key1=val1&key2=val2...). This method translates that
     * encoding to a map, mapping from the keys found in the opaque string to
     * the values found in the opaque string.
     *
     * Due to ambiguity regarding specification of the opaque token and its
     * use, the method will parse opaque strings in the forms
     *
     *  ?firstKey=firstValue&secondKey=secondValue
     *  ?&firstKey=firstValue&secondKey=secondValue
     *  firstKey=firstValue?&secondKey=secondValue
     *  firstKey=firstValue?secondkey=secondValue
     *
     * @param opaque The opaque string, as usually attached to the path
     * @return Map from keys to values in the opaque string
     * @throws ParseException if value is missing for a key in the string
     */
    public static Map<String,String> getOpaqueMap(String opaque)
                                                        throws ParseException
    {
        if (opaque == null || opaque.isEmpty()) {
            return Collections.emptyMap();
        } else {
            Map<String,String> map = new HashMap<String,String>();

            String [] prefixBlocks = opaque.split("\\?");

            for (String prefixBlock : prefixBlocks) {

                if (prefixBlock.isEmpty()) {
                    continue;
                }

                String [] prefixSubBlocks = prefixBlock.split("\\&");

                for (String prefixSubBlock : prefixSubBlocks) {

                    if (prefixSubBlock.isEmpty()) {
                        continue;
                    }

                    int delimiter = prefixSubBlock.indexOf(OPAQUE_SEPARATOR);
                    if (delimiter == -1) {
                        throw new ParseException("Opaque information is missing a"
                                                 + "value for variable " +
                                                  prefixSubBlock);
                    }

                    map.put(prefixSubBlock.substring(0, delimiter),
                            prefixSubBlock.substring(delimiter + 1, prefixSubBlock.length()));
                }
            }

            return map;
        }
    }

    /**
     * Build an opaque string containing a single key and a single value
     * @param key The key contained in the opaque string
     * @param value The value contained in the opaque string
     * @return string with correct opaque prefix and correct separator
     */
    public static String buildOpaqueString(String key, String value)
    {
        return OPAQUE_PREFIX + key + OPAQUE_SEPARATOR + value;
    }
}
