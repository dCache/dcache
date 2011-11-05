package org.dcache.util;

import java.util.Properties;
import java.util.HashSet;
import java.util.Set;

/**
 * ConfigurationProperties with scope resolution.
 *
 * A scoped name begins with the name of the scope followed by the
 * scoping operator, a forward slash. A scoped declaration only
 * affects the evaluation within the named scope.
 *
 * In scope S, a property P will be evaluted as follows:
 *
 * - if P is declared in scope S to value A, that is, declared in this
 *   instance of ScopedConfigurationProperties as opposed to in the
 *   default Properties provided during construction then P evaluates
 *   to A. Otherwise
 *
 * - if S/P is declared to value B, either in this instance or as a
 *   default, then P evaluates to B. Otherwise
 *
 * - if P is declared to value C as a default, then P evalutes to C.
 */
public class ScopedConfigurationProperties extends ConfigurationProperties
{
    private final String _scope;

    public ScopedConfigurationProperties(Properties properties, String scope)
    {
        super(properties);
        _scope = scope;
    }

    /**
     * Returns whether a name is scoped.
     *
     * A scoped name begins with the name of the scope followed by the
     * scoping operator, a forward slash.
     */
    public static boolean isScoped(String name)
    {
        return name.indexOf('/') > -1;
    }

    /**
     * Returns whether a name has a particular scope.
     */
    public static boolean isScoped(String scope, String name)
    {
        return scope.length() < name.length() &&
            name.startsWith(scope) && name.charAt(scope.length()) == '/';
    }

    /**
     * Returns the unscoped name.
     */
    public static String stripScope(String name)
    {
        int pos = name.indexOf('/');
        return (pos == -1) ? name : name.substring(pos + 1);
    }

    @Override
    public Set<String> stringPropertyNames()
    {
        Set<String> names = new HashSet<String>();
        for (String name: super.stringPropertyNames()) {
            if (!isScoped(name)) {
                names.add(name);
            } else if (isScoped(_scope, name)) {
                names.add(stripScope(name));
            }
        }
        return names;
    }

    @Override
    public String getProperty(String key)
    {
        String value = (String) get(key);
        if (value != null) {
            return value;
        }

        value = super.getProperty(_scope + "/" + key);
        if (value != null) {
            return value;
        }

        return super.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue)
    {
        String value = getProperty(key);
        return (value == null) ? defaultValue : value;
    }

    @Override
    public Object put(Object rawKey, Object value)
    {
        AnnotatedKey key = new AnnotatedKey(rawKey, value);

        if (!isScoped(key.getPropertyName())) {
            AnnotatedKey scopedKey = new AnnotatedKey(_scope + "/" + key.getPropertyName(), value);
            checkIsAllowedKey(scopedKey);
        }

        return super.put(rawKey, value);
    }
}