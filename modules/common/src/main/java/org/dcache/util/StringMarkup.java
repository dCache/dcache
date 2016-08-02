package org.dcache.util;

import com.google.common.base.CharMatcher;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class provides utility methods for encoding UTF-8 String data using
 * various formats.  The methods either provide missing functionality or
 * contain work-arounds for buggy library implementations.
 *
 * Most methods provide a method with a signature that takes a StringBuilder
 * to which the encoded form is appended, and a method with the same name
 * but that returns the encoded value as a String.
 */
public class StringMarkup
{
    // See RFC 822 for definition of quoted-string special
    private static final CharMatcher QUOTE_STRING_SPECIAL =
            CharMatcher.anyOf("\\\"");

    private static final String SCHEME_FILE = "file";
    private static final int SCHEME_FILE_LENGTH = SCHEME_FILE.length();


    /**
     * Provides the quoted-string markup, as defined in RFC 822.  This is
     * a simple markup where '\' before any character makes that character
     * a literal.
     *
     * Any occurrence of a backslash or double-quote character is marked up and
     * the resulting string is placed in double-quotes.
     * @param sb The StringBuilder to append the marked-up value
     * @param src The unencoded string.
     * @return the StringBuilder.
     */
    public static StringBuilder quotedString(StringBuilder sb, String src)
    {
        sb.append('\"');
        for(int i = 0; i < src.length(); i++) {
            char c = src.charAt(i);
            if(QUOTE_STRING_SPECIAL.matches(c)) {
                sb.append('\\');
            }
            sb.append(c);
        }
        sb.append('\"');

        return sb;
    }


    /**
     * Provides the quoted-string markup, as defined in RFC 822.  This is
     * a simple markup where '\' before any character makes that character
     * a literal.
     *
     * Any occurrence of a backslash or double-quote character is marked up and
     * the resulting string is placed in double-quotes.
     * @param src The unencoded string.
     * @return the encoded string.
     */
    public static String quotedString(String src)
    {
        return quotedString(new StringBuilder(), src).toString();
    }


    /**
     * The string is encoded by mapping the characters to bytes using UTF-8
     * and any reserved characters are marked up using percent symbol followed
     * by two hexadecimal digits from the set {'0'-'9', 'A'-'F'}.
     * This is in accordance with RFC 3986.
     * @param sb The StringBuilder to append the marked-up value
     * @param src The unencoded string.
     * @return the StringBuilder.
     * @throws RuntimeException if the path is somehow illegal.
     */
    public static StringBuilder percentEncode(StringBuilder sb, String src)
    {
        return sb.append(percentEncode(src));
    }


    /**
     * The string is encoded by mapping the characters to bytes using UTF-8
     * and any reserved characters are marked up using percent symbol followed
     * by two hexadecimal digits from the set {'0'-'9', 'A'-'F'}.
     * This is in accordance with RFC 3986.
     * @param sb The StringBuilder to append the marked-up value
     * @param src The unencoded string.
     * @return the StringBuilder.
     * @throws RuntimeException if the path is somehow illegal.
     */
    public static String percentEncode(String src)
    {
        URI uri;

        /*
         * This method contains a work-around for a JRE bug:
         *
         *     https://bugs.openjdk.java.net/show_bug.cgi?id=100223
         *
         * We should be able to use the four-argument constructor to obtain
         * the encoded form of the path element:
         *
         *   uri = new URI(null, null, path, null);
         *   uri.toASCIIString()
         *
         * However, this can fail if the path contains a colon.  Instead, we
         * use the "file" scheme and ensure the path is absolute.  The code
         * then strips off the initial "file:/" to obtain the encoded path.
         */

        try {
            uri = new URI(SCHEME_FILE, null, '/' + src, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("illegal path element: " +
                    e.getMessage(), e);
        }

        String encoded = uri.toASCIIString();

        int idx = SCHEME_FILE_LENGTH +2; // +2 for ':/' in 'file:/'

        return encoded.substring(idx, encoded.length());
    }
}
