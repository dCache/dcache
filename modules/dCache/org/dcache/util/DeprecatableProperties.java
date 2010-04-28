package org.dcache.util;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplaceableProperties that allows properties to be annotated as
 * deprecated, obsolete or forbidden. A property is annotated as deprecated
 * by prefixing the declaration with <code>(deprecated)</code>, as obsolete
 * by prefixing the declaration with <code>(obsolete)</code>, and forbidden
 * by prefixing it <code>(forbidden)</code>.
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
 */
public class DeprecatableProperties extends ReplaceableProperties {
    private static final long serialVersionUID = -5684848160314570455L;

    private final static Logger _log =
        LoggerFactory.getLogger(DeprecatableProperties.class);

    private final static String FORBIDDEN = "(forbidden)";
    private final static String OBSOLETE = "(obsolete)";
    private final static String DEPRECATED = "(deprecated)";

    public DeprecatableProperties(Properties properties)
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

    @Override
    public synchronized Object put(Object key, Object value)
    {
        String s = key.toString();
        actOnAnnotation(s);
        if( isDeprecatedDeclaration(s)) {
            super.put(s.substring(DEPRECATED.length()), value);
            }
        return super.put(key, value);
    }

    private void actOnAnnotation(String key)
    {
        if( isForbidden( key)) {
            String error = errorForForbiddenProperty( key);
            throw new IllegalArgumentException(error);
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
        Set<String> names = super.stringPropertyNames();
        Iterator<String> i = names.iterator();
        while (i.hasNext()) {
            String name = i.next();
            if( isAnnotatedDeclaration(name)) {
                i.remove();
            }
        }
        return names;
    }

    private boolean isDeprecatedDeclaration(String name)
    {
        return name.startsWith(DEPRECATED);
    }

    private boolean isAnnotatedDeclaration(String name)
    {
        boolean isDeprecated = isDeprecatedDeclaration( name);

        boolean isForbidden = name.startsWith(FORBIDDEN);
        boolean isObsolete = name.startsWith(OBSOLETE);

        return isForbidden || isDeprecated || isObsolete;
    }
}