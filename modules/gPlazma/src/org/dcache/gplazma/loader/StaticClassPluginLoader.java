package org.dcache.gplazma.loader;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * This class provides a mechanism for loading GPlazma plugins from a static
 * list of available plugins.  This class is meant to demonstrate the
 * PluginLoader interface and to allow simple testing.
 * <p>
 * The plugin name are taken from the plugin class' simple class name.
 */
public class StaticClassPluginLoader extends AbstractPluginLoader {
    private final PluginRepositoryFactory _repositoryFactory;

    @SuppressWarnings("unchecked")
    public static PluginLoader newPluginLoader(Class<? extends GPlazmaPlugin> plugin)
    {
        Class[] plugins = new Class[] { plugin };
        PluginLoader inner = new StaticClassPluginLoader(plugins);
        PluginLoader outer = new SafePluginLoaderDecorator(inner);
        return outer;
    }

    public static PluginLoader newPluginLoader(Class<? extends GPlazmaPlugin>... plugins) {
        PluginLoader inner = new StaticClassPluginLoader(plugins);
        PluginLoader outer = new SafePluginLoaderDecorator( inner);
        return outer;
    }

    private StaticClassPluginLoader(Class<? extends GPlazmaPlugin>... plugins) {
        _repositoryFactory = new StaticClassPluginRepositoryFactory(plugins);
    }

    @Override
    PluginRepositoryFactory getRepositoryFactory() {
        return _repositoryFactory;
    }
}
