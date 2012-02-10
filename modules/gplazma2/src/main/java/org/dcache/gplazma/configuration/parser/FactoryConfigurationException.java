package org.dcache.gplazma.configuration.parser;

import org.dcache.gplazma.GPlazmaInternalException;

public class FactoryConfigurationException extends GPlazmaInternalException
{
    static final long serialVersionUID = -465585959811498555L;

    public FactoryConfigurationException(String msg)
    {
        super(msg);
    }

    public FactoryConfigurationException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
