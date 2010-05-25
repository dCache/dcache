package org.dcache.gplazma.loader;

import java.util.HashMap;
import java.util.Map;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * This class provides a mechanism for loading GPlazma plugins from a static
 * list of available plugins.  This class is meant to demonstrate the
 * PluginLoader interface and to allow simple testing.
 * <p>
 * The plugin name are taken from the plugin class' simple class name.
 */
public class StaticClassPluginLoader implements PluginLoader {

    public static PluginLoader newPluginLoader(Class<? extends GPlazmaPlugin>... plugins) {
        PluginLoader inner = new StaticClassPluginLoader(plugins);
        PluginLoader outer = new SafePluginLoaderDecorator(inner);
        return outer;
    }

    private PluginFactory _factory = new StringArrayPluginFactory();
    protected Class<? extends GPlazmaPlugin> _plugins[];

    private final Map<String, Class<? extends GPlazmaPlugin>> _pluginClassByName =
            new HashMap<String, Class<? extends GPlazmaPlugin>>();

    private StaticClassPluginLoader(Class<? extends GPlazmaPlugin>... plugins) {
        _plugins = plugins;
    }

    @Override
    public void init() {
        for( Class<? extends GPlazmaPlugin> pluginClass : _plugins) {
            String pluginName = getNameFromClass( pluginClass);
            _pluginClassByName.put( pluginName, pluginClass);
        }
    }

    protected String getNameFromClass(Class<? extends GPlazmaPlugin> pluginClass) {
        return pluginClass.getSimpleName();
    }

    @Override
    public GPlazmaPlugin newPluginByName(String pluginName) {
        Class<? extends GPlazmaPlugin> pluginClass =
            getPluginClassByPluginName( pluginName);
        GPlazmaPlugin plugin = _factory.newPlugin( pluginClass);
        return plugin;
    }

    @Override
    public GPlazmaPlugin newPluginByName(String pluginName, String[] arguments) {
        Class<? extends GPlazmaPlugin> pluginClass =
                getPluginClassByPluginName( pluginName);
        GPlazmaPlugin plugin = _factory.newPlugin( pluginClass, arguments);
        return plugin;
    }

    private Class<? extends GPlazmaPlugin> getPluginClassByPluginName(String pluginName) {
        Class<? extends GPlazmaPlugin> pluginClass =
                _pluginClassByName.get( pluginName);

        if( pluginClass == null) {
            throw new IllegalArgumentException("No such plugin with name " +
                                               pluginName);
        }

        return pluginClass;
    }
}
