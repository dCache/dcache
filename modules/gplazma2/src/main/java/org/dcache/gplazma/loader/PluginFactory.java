package org.dcache.gplazma.loader;

import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * A Class implementing PluginFactory is responsible for instantiating a
 * GPlazmaPlugin from the Class object. This involves calling the correct
 * constructor (via reflection) and passing arguments to the constructor, if
 * any.
 */
public interface PluginFactory
{
    /**
     * Create a new plugin of the given type.
     * @param pluginClass the Class of plugin to create
     * @return the new plugin object
     * @throws PluginLoadingException if creation was impossible
     */
    <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass)
            throws PluginLoadingException;

    /**
     * Create a new plugin of the given type.
     * @param pluginClass the Class of plugin to create
     * @param properties an array of Strings as arguments for the plugin
     * @return the new plugin object
     * @throws PluginLoadingException if creation was impossible
     */
    <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass,
                                          Properties properties) throws PluginLoadingException;
}
