package org.dcache.util;

import com.google.common.base.CharMatcher;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * A glob is a pattern used for limited pattern matching. The class
 * supports the following glob patterns:
 *
 * <ul>
 * <li><tt>?</tt> Matches any single character.</li>
 * <li><tt>*</tt> Matches any sequence of zero or more characters.</li>
 * <li><tt>{a,b,...}</tt> Matches any of the sub-patterns a, b, etc.</li>
 * </ul>
 *
 * The current implementation does not have an escape symbol.
 */
public class Glob implements Serializable
{
    private static final long serialVersionUID = -5052804169005574207L;
    private static final CharMatcher WILDCARD = CharMatcher.anyOf("*?{");

    private final String _pattern;

    public Glob(String s)
    {
        _pattern = s;
    }

    public boolean matches(String s)
    {
        return toPattern().matcher(s).matches();
    }

    public boolean isGlob()
    {
        return isGlob(_pattern);
    }

    @Override
    public String toString()
    {
        return _pattern;
    }

    public Pattern toPattern()
    {
        return parseGlobToPattern(_pattern);
    }

    public static Pattern parseGlobToPattern(String glob)
    {
        StringBuilder s = new StringBuilder(glob.length() * 2 + 2);
        int j = 0;
        s.append("^");
        for (int i = 0; i < glob.length(); i++) {
            switch (glob.charAt(i)) {
            case '?':
                s.append(Pattern.quote(glob.substring(j, i)));
                s.append(".");
                j = i + 1;
                break;

            case '*':
                s.append(Pattern.quote(glob.substring(j, i)));
                s.append(".*");
                j = i + 1;
                break;

            case '{':
                s.append(Pattern.quote(glob.substring(j, i)));
                i = parseCurlyBrackets(glob, i, s);
                j = i + 1;
                break;

            default:
                break;
            }
        }
        s.append(Pattern.quote(glob.substring(j)));
        s.append("$");
        return Pattern.compile(s.toString());
    }

    private static int parseCurlyBrackets(String glob, int from, StringBuilder out)
    {
        assert glob.charAt(from) == '{';

        StringBuilder s = new StringBuilder();
        int j = from + 1;
        for (int i = j; i < glob.length(); i++) {
            switch (glob.charAt(i)) {
            case '?':
                s.append(Pattern.quote(glob.substring(j, i)));
                s.append(".");
                j = i + 1;
                break;

            case '*':
                s.append(Pattern.quote(glob.substring(j, i)));
                s.append(".*");
                j = i + 1;
                break;

            case ',':
                s.append(Pattern.quote(glob.substring(j, i)));
                s.append('|');
                j = i + 1;
                break;

            case '{':
                s.append(Pattern.quote(glob.substring(j, i)));
                i = parseCurlyBrackets(glob, i, s);
                j = i + 1;
                break;

            case '}':
                out.append('(').append(s).append(Pattern.quote(glob.substring(j, i))).append(')');
                return i;

            default:
                break;
            }
        }

        // Unterminated curly brace
        out.append(Pattern.quote("{"));
        return from;
    }

    public static boolean isGlob(String s)
    {
        return WILDCARD.matchesAnyOf(s);
    }

    /**
     * Unfolds alternations (brace lists) into a list.
     */
    public static Iterable<String> expandGlob(String glob)
    {
        return new GlobBraceParser(glob).expandGlob();
    }

    /**
     * Similar to {@code expandGlob} but considers the input
     * string to be a comma separated list. Equivalient to calling
     * {@code expandGlob("{" + glob + "}")}.
     */
    public static Iterable<String> expandList(String glob)
    {
        return new GlobBraceParser(glob).expandList();
    }
}
