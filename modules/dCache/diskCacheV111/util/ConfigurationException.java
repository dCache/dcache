package diskCacheV111.util;

/**
 * Thrown when a configuration error is found.
 */
public class ConfigurationException extends Exception
{
    public ConfigurationException(String msg)
    {
        super(msg);
    }

    public ConfigurationException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}

