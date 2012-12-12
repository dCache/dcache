package org.dcache.gplazma.loader;

import diskCacheV111.namespace.NameSpaceProvider;
import java.util.Properties;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.dcache.gplazma.plugins.NamespaceAware;

/**
 *  A PluginLoader that allows gPlazma plugins to be dCache aware.
 */
public class DcacheAwarePluginFactory extends PropertiesPluginFactory
{
    private final NameSpaceProvider _namespace;

    public DcacheAwarePluginFactory(NameSpaceProvider namespace)
    {
        _namespace = namespace;
    }

    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass)
            throws PluginLoadingException
    {
        T plugin = super.newPlugin(pluginClass);
        makePluginDcacheAware(plugin);
        return plugin;
    }


    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass,
            Properties properties) throws PluginLoadingException
    {
        T plugin = super.newPlugin(pluginClass, properties);
        makePluginDcacheAware(plugin);
        return plugin;
    }

    private <T extends GPlazmaPlugin> void makePluginDcacheAware(T plugin)
    {
        if(plugin instanceof NamespaceAware) {
            NamespaceAware nsPlugin = (NamespaceAware) plugin;
            nsPlugin.setNamespace(_namespace);
        }
    }
}
