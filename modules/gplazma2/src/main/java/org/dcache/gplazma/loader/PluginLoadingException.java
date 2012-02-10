package org.dcache.gplazma.loader;

import org.dcache.gplazma.GPlazmaInternalException;

/**
 * This Exception indicates that there was a problem loading
 * a plugin.
 */
public class PluginLoadingException extends GPlazmaInternalException
{
    static final long serialVersionUID = -7308354490378360208L;

    public PluginLoadingException(Throwable cause)
    {
        super(cause);
    }

    public PluginLoadingException(String msg)
    {
        super(msg);
    }

    public PluginLoadingException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
