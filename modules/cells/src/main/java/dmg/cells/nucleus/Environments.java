package dmg.cells.nucleus;

import java.util.Map;
import java.util.Properties;

import dmg.util.Formats;
import dmg.util.Replaceable;

/**
 * Utility class for working with a Cell's environment
 */
public class Environments
{
    private Environments()
    {
        // prevent instantiation
    }

    public static Properties toProperties(final Map<String,Object> env)
    {
        Replaceable replaceable = new Replaceable() {
            @Override
            public String getReplacement(String name)
            {
                Object value =  env.get(name);
                return (value == null) ? null : value.toString().trim();
            }
        };

        Properties properties = new Properties();
        for (Map.Entry<String,Object> e: env.entrySet()) {
            String key = e.getKey();
            String value = String.valueOf(e.getValue());
            properties.put(key, Formats.replaceKeywords(value, replaceable));
        }

        return properties;
    }
}
