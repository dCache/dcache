package org.dcache.gplazma.loader;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 *
 * @author timur
 */
public class CachingPluginLoaderDecorator implements PluginLoader {

    private final PluginLoader pluginLoader;
    private Map<String,GPlazmaPlugin> foundPlugins;

    public CachingPluginLoaderDecorator(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    @Override
    public void init() {
        pluginLoader.init();
        foundPlugins = new HashMap<String, GPlazmaPlugin>();
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name) {
        String key = name;

        GPlazmaPlugin plugin = foundPlugins.get(key);
        if(plugin != null) {
            return plugin;
        }

        plugin = pluginLoader.newPluginByName(name);
        if(plugin == null) {
            throw new IllegalArgumentException("plugin "+name+" can not be loaded");
        }
        foundPlugins.put(key, plugin);
        return plugin;
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name, Properties properties) {

        String key = name;

        GPlazmaPlugin plugin = foundPlugins.get(key);
        if(plugin != null) {
            return plugin;
        }

        plugin = pluginLoader.newPluginByName(name, properties);
        if(plugin == null) {
            throw new IllegalArgumentException("plugin "+name+" can not be loaded");
        }
        foundPlugins.put(key, plugin);
        return plugin;
    }

}
