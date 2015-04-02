package diskCacheV111.util;

/**
 * Thrown when a configuration error is found.
 */
public class ConfigurationException extends Exception
{
    private static final long serialVersionUID = -1879022223068061993L;

    public ConfigurationException(String msg)
    {
        super(msg);
    }

    public ConfigurationException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}

