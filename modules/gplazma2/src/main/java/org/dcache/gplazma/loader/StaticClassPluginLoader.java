package org.dcache.gplazma.loader;

import com.google.common.collect.ImmutableList;

import java.util.Collection;

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

    public static PluginLoader newPluginLoader(Class<? extends GPlazmaPlugin> plugin)
    {
        ImmutableList.Builder<Class<? extends GPlazmaPlugin>> b = ImmutableList.builder();
        b.add(plugin);
        Collection<Class<? extends GPlazmaPlugin>> plugins = b.build();
        return new SafePluginLoaderDecorator(new StaticClassPluginLoader(plugins));
    }

    public static PluginLoader newPluginLoader(Collection<Class<? extends GPlazmaPlugin>> plugins) {
        return new SafePluginLoaderDecorator(new StaticClassPluginLoader(plugins));
    }

    private StaticClassPluginLoader(Collection<Class<? extends GPlazmaPlugin>> plugins) {
        _repositoryFactory = new StaticClassPluginRepositoryFactory(plugins);
    }

    @Override
    PluginRepositoryFactory getRepositoryFactory() {
        return _repositoryFactory;
    }
}
