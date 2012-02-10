package org.dcache.gplazma.configuration;

import org.dcache.gplazma.GPlazmaInternalException;

/**
 * Implementation of this interface will be used by GPlazma class to
 * load and keep up to date the configuration
 * @author timur
 */
public interface ConfigurationLoadingStrategy
{
    /**
     * @return latest configuration
     */
    public Configuration load() throws GPlazmaInternalException;

    /**
     * @return true if the configuration has been updated since the load method
     * has been called last time
     */
    public boolean hasUpdated();
}
