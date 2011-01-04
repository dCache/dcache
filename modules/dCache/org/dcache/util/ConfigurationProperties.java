package org.dcache.util;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;
import java.io.InputStream;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.util.Replaceable;
import dmg.util.Formats;
import dmg.util.PropertiesBackedReplaceable;

/**
 * The ConfigurationProperties class represents a set of dCache
 * configuration properties.
 *
 * Repeated declaration of the same property is considered an error
 * and will cause loading of configuration files to fail.
 *
 * Properties can be annotated as deprecated, obsolete or forbidden. A
 * property is annotated as deprecated by prefixing the declaration
 * with <code>(deprecated)</code>, as obsolete by prefixing the
 * declaration with <code>(obsolete)</code>, and forbidden by
 * prefixing it <code>(forbidden)</code>.
 *
 * These annotations have the following semantics:
 * <ul>
 * <li><i>deprecated</i> indicates that a property is supported but that a
 * future version of dCache will likely remove that support.
 * <li><i>obsolete</i> indicates that a property is no longer supported and
 * that dCache will always behaves correctly without supporting this
 * property.
 * <li><i>forbidden</i> indicates that a property is no longer supported and
 * dCache does not always behave correctly without further configuration or
 * that support for some feature has been removed.
 * </ul>
 * <p>
 * The intended behaviour of dCache when encountering sysadmin-supplied
 * property assignment of some annotated property is dependent on the
 * annotation. For deprecated and obsolete properties, a warning is emitted
 * and dCache continues to start up. If the user assigns a value to a
 * forbidden properties then dCache will refuse to start.
 * <p>
 * Such a declaration only affects following declarations. It does not affect
 * any previous declarations of this property, nor does it generate any
 * errors when such properties are referenced in any way.
 *
 * @see java.util.Properties
 */
public class ConfigurationProperties
    extends Properties
{
    private static final long serialVersionUID = -5684848160314570455L;

    private final static Logger _log =
        LoggerFactory.getLogger(ConfigurationProperties.class);

    private final static String FORBIDDEN = "(forbidden)";
    private final static String OBSOLETE = "(obsolete)";
    private final static String DEPRECATED = "(deprecated)";

    private final PropertiesBackedReplaceable _replaceable =
        new PropertiesBackedReplaceable(this);

    private boolean _loading = false;

    public ConfigurationProperties()
    {
        super();
    }

    public ConfigurationProperties(Properties properties)
    {
        super(properties);
    }

    public boolean isForbidden(String key)
    {
        return getProperty(FORBIDDEN + key) != null;
    }

    public boolean isObsolete(String key)
    {
        return getProperty(OBSOLETE + key) != null;
    }

    public boolean isDeprecated(String key)
    {
        return getProperty(DEPRECATED + key) != null;
    }

    /**
     * Returns whether a name is scoped.
     *
     * A scoped name begins with the name of the scope followed by the
     * scoping operator, a forward slash.
     */
    public static boolean isScoped(String key)
    {
        return key.indexOf('/') > -1;
    }

    /**
     * @throws IllegalArgumentException during loading if a property
     * is defined multiple times.
     */
    @Override
    public synchronized void load(Reader reader) throws IOException
    {
        _loading = true;
        try {
            super.load(reader);
        } finally {
            _loading = false;
        }
    }

    /**
     * @throws IllegalArgumentException during loading if a property
     * is defined multiple times.
     */
    @Override
    public synchronized void load(InputStream in) throws IOException
    {
        _loading = true;
        try {
            super.load(in);
        } finally {
            _loading = false;
        }
    }

    /**
     * @throws IllegalArgumentException during loading if a property
     * is defined multiple times.
     */
    @Override
    public synchronized void loadFromXML(InputStream in)
        throws IOException, InvalidPropertiesFormatException
    {
        _loading = true;
        try {
            super.loadFromXML(in);
        } finally {
            _loading = false;
        }
    }

    /**
     * Loads a Java properties file.
     */
    public void loadFile(File file)
        throws IOException
    {
        Reader in = new FileReader(file);
        try {
            load(in);
        } finally {
            in.close();
        }
    }

    /**
     * @throws IllegalArgumentException during loading if key is
     * already defined.
     */
    @Override
    public synchronized Object put(Object key, Object value)
    {
        if (_loading && containsKey(key)) {
            throw new IllegalArgumentException(String.format("%s is already defined", key));
        }
        String s = key.toString();
        actOnAnnotation(s);
        if (isDeprecatedDeclaration(s)) {
            super.put(s.substring(DEPRECATED.length()), value);
        }
        return super.put(key, value);
    }

    private void actOnAnnotation(String key)
    {
        if( isForbidden( key)) {
            String error = errorForForbiddenProperty( key);
            throw new IllegalArgumentException( error);
        }

        if( isObsolete( key)) {
            _log.warn( "The property {} is no longer used.", key);
        } else if( isDeprecated( key)) {
            _log.warn( "The property {} is deprecated and will be removed.", key);
        }
    }

    private String errorForForbiddenProperty(String property)
    {
        String error = getProperty( FORBIDDEN + property);

        if( error.isEmpty()) {
            error = "Adjusting property " + property + " is forbidden as different properties now control this aspect of dCache.";
        }

        return error;
    }

    @Override
    public Enumeration<?> propertyNames()
    {
        return Collections.enumeration(stringPropertyNames());
    }

    @Override
    public Set<String> stringPropertyNames()
    {
        Set<String> names = new HashSet<String>();
        for (String name: super.stringPropertyNames()) {
            if (!isAnnotatedDeclaration(name)) {
                names.add(name);
            }
        }
        return names;
    }

    private boolean isDeprecatedDeclaration(String name)
    {
        return name.startsWith(DEPRECATED);
    }

    private boolean isForbiddenDeclaration(String name)
    {
        return name.startsWith(FORBIDDEN);
    }

    private boolean isObsoleteDeclaration(String name)
    {
        return name.startsWith(OBSOLETE);
    }

    private boolean isAnnotatedDeclaration(String name)
    {
        boolean isDeprecated = isDeprecatedDeclaration(name);
        boolean isForbidden = isForbiddenDeclaration(name);
        boolean isObsolete = isObsoleteDeclaration(name);
        return isForbidden || isDeprecated || isObsolete;
    }

    public String replaceKeywords(String s)
    {
        return Formats.replaceKeywords(s, _replaceable);
    }

    public String getValue(String name)
    {
        String value = getProperty(name);
        return (value == null) ? null : replaceKeywords(value);
    }
}