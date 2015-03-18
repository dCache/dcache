package dmg.cells.nucleus;

import java.util.Map;
import java.util.Properties;

import dmg.util.Formats;

/**
 * Utility class for working with a Cell's environment
 */
public class Environments
{

    private Environments()
    {
        // prevent instantiation
    }

    public static Properties toProperties(Map<String,Object> env)
    {
        Properties properties = new Properties();

        env.forEach((k,v) -> properties.put(k, expand(v, env)));

        return properties;
    }

    public static String getValue(Map<String,Object> env, String name)
    {
        Object value = env.get(name);

        if (value == null) {
            throw new IllegalArgumentException("'" + name + "' is not set");
        }

        if (!(value instanceof String)) {
            throw new IllegalArgumentException("Invalid value of '" + name +
                    "': " + value);
        }

        return expand(value, env);
    }

    private static String expand(Object in, Map<String,Object> env)
    {
        return Formats.replaceKeywords(String.valueOf(in), n -> {
                    Object t = env.get(n);
                    return t == null ? null : t.toString().trim();
                });
    }
}
