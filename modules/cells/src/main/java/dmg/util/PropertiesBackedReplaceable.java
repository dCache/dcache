package dmg.util;

import java.util.Properties;

/**
 * ReplaceableProperties wraps Properties.
 */
public class PropertiesBackedReplaceable
    implements Replaceable
{
    private final Properties _properties;

    public PropertiesBackedReplaceable(Properties properties)
    {
        _properties = properties;
    }

    /**
     * Returns the value of a property with all placeholders in the
     * value substituted recursively.
     */
    @Override
    public synchronized String getReplacement(String name)
    {
        return _properties.getProperty(name);
    }
}
