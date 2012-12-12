package org.dcache.gplazma.loader;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * This class provides a wrapper around any PluginLoader so that
 * repeated attempts to instantiate plugins with the same name return
 * the same object.
 */
public class CachingPluginLoaderDecorator implements PluginLoader
{

    private final PluginLoader pluginLoader;
    private Map<String,GPlazmaPlugin> foundPlugins;

    public CachingPluginLoaderDecorator(PluginLoader pluginLoader)
    {
        this.pluginLoader = pluginLoader;
    }

    @Override
    public void setPluginFactory(PluginFactory factory)
    {
        pluginLoader.setPluginFactory(factory);
    }

    @Override
    public void init()
    {
        pluginLoader.init();
        foundPlugins = new HashMap<>();
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name)
            throws PluginLoadingException
    {
        String key = name;

        GPlazmaPlugin plugin = foundPlugins.get(key);
        if(plugin != null) {
            return plugin;
        }

        plugin = pluginLoader.newPluginByName(name);

        foundPlugins.put(key, plugin);
        return plugin;
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name, Properties properties)
            throws PluginLoadingException
    {

        String key = name;

        GPlazmaPlugin plugin = foundPlugins.get(key);
        if(plugin != null) {
            return plugin;
        }

        plugin = pluginLoader.newPluginByName(name, properties);

        foundPlugins.put(key, plugin);
        return plugin;
    }
}
