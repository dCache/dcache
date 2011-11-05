package org.dcache.gplazma.configuration.parser;

public class FactoryConfigurationError  extends Error
{
    static final long serialVersionUID = -465585959811498555L;

    public FactoryConfigurationError(String msg) {
        super(msg);
    }

    public FactoryConfigurationError(String msg, Throwable cause) {
        super(msg, cause);
    }
}
