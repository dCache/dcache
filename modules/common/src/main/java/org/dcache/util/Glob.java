package org.dcache.util;

import com.google.common.base.CharMatcher;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * A glob is a pattern used for limitted pattern matching. The class
 * supports single character (question mark) and multi character
 * (asterix) wildcards, similar to Unix shell globbing.
 *
 * Due to its simplicity, a glob is easy to translate to other pattern
 * matching language.
 *
 * The current implementation does not have an escape symbol.
 */
public class Glob implements Serializable
{
    private static final long serialVersionUID = -5052804169005574207L;
    private static final CharMatcher WILDCARD = CharMatcher.anyOf("*?");

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
        return WILDCARD.matchesAnyOf(_pattern);
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

    public String toSql()
    {
        return parseGlobToSql(_pattern);
    }

    public static String parseGlobToSql(String glob)
    {
        StringBuilder s = new StringBuilder(glob.length() * 2 + 2);
        int j = 0;
        for (int i = 0; i < glob.length(); i++) {
            switch (glob.charAt(i)) {
                case '?':
                    s.append(quoteSql(glob.substring(j, i)));
                    s.append("_");
                    j = i + 1;
                    break;

                case '*':
                    s.append(quoteSql(glob.substring(j, i)));
                    s.append("%");
                    j = i + 1;
                    break;

                default:
                    break;
            }
        }
        s.append(quoteSql(glob.substring(j)));
        return s.toString();
    }

    private static String quoteSql(String s)
    {
        return s.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%");
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

            default:
                break;
            }
        }
        s.append(Pattern.quote(glob.substring(j)));
        s.append("$");
        return Pattern.compile(s.toString());
    }
}
