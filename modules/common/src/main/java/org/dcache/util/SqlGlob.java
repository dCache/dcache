package org.dcache.util;

import com.google.common.base.CharMatcher;

import java.io.Serializable;

/**
 * A glob is a pattern used for limited pattern matching. The class
 * supports single character (question mark) and multi character
 * (asterix) wildcards, similar to Unix shell globing.
 *
 * Due to its simplicity, a glob is easy to translate to other pattern
 * matching language. This particular implementation supports translation
 * to SQL LIKE wildcards.
 *
 * This class is separate from the Glob class because the translation to
 * SQL LIKE patterns restricts the range of Globs we can easily translate.
 */
public class SqlGlob implements Serializable
{
    private static final long serialVersionUID = -5052804169005574207L;
    private static final CharMatcher WILDCARD = CharMatcher.anyOf("*?");

    private final String _pattern;

    public SqlGlob(String s)
    {
        _pattern = s;
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

    public Glob toGlob()
    {
        return new Glob(_pattern);
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

    public static boolean isGlob(String s)
    {
        return WILDCARD.matchesAnyOf(s);
    }
}
