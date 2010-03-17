package org.dcache.util;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Collections;
import java.util.Iterator;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplaceableProperties that allows properties to be declared
 * deprecated or obsolete. Deprecated properties omit a warning when
 * declared and obsolete properties causes an IllegalArgumentException
 * to be thrown.
 *
 * A property is declared deprecated or obsolete by prefixing them
 * with '(deprecated)' and '(obsolete)'. The value of a obsolete
 * property is used as a error message.
 *
 * Such a declaration only affects following declarations. It does not
 * affect any previous declarations of this property, nor does it
 * generate any errors when such properties are referenced in any
 * way.
 */
public class DeprecatableProperties extends ReplaceableProperties
{
    private final static Logger _log =
        LoggerFactory.getLogger(DeprecatableProperties.class);

    private final static String OBSOLETE = "(obsolete)";
    private final static String DEPRECATED = "(deprecated)";

    public DeprecatableProperties(Properties properties)
    {
        super(properties);
    }

    public boolean isObsolete(String key)
    {
        return getProperty(OBSOLETE + key) != null;
    }

    public boolean isDeprecated(String key)
    {
        return getProperty(DEPRECATED + key) != null;
    }

    @Override
    public synchronized Object put(Object key, Object value)
    {
        String s = key.toString();
        if (isObsolete(s)) {
            String error = getProperty(OBSOLETE + s);
            if (error.isEmpty()) {
                error = "The property " + s + " is obsolete.";
            }
            throw new IllegalArgumentException(error);
        }
        if (isDeprecated(s)) {
            _log.warn("The property {} is deprecated and will be removed.", s);
        }
        if (s.startsWith(DEPRECATED)) {
            super.put(s.substring(DEPRECATED.length()), value);
        }
        return super.put(key, value);
    }

    @Override
    public Enumeration<?> propertyNames()
    {
        return Collections.enumeration(stringPropertyNames());
    }

    @Override
    public Set<String> stringPropertyNames()
    {
        Set<String> names = super.stringPropertyNames();
        Iterator<String> i = names.iterator();
        while (i.hasNext()) {
            String name = i.next();
            if (name.startsWith(OBSOLETE) || name.startsWith(DEPRECATED)) {
                i.remove();
            }
        }
        return names;
    }
}