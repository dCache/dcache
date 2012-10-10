package dmg.util;

import java.io.Serializable;
import java.util.Properties;

/**
 * ReplaceableProperties wraps Properties.
 */
public class PropertiesBackedReplaceable
    implements Replaceable, Serializable
{
    private static final long serialVersionUID = 1255254024789786471L;

    private final Properties _properties;

    public PropertiesBackedReplaceable(Properties properties)
    {
        _properties = properties;
    }

    @Override
    public synchronized String getReplacement(String name)
    {
        String value = _properties.getProperty(name);
        return value == null ? null : value.trim();
    }
}
