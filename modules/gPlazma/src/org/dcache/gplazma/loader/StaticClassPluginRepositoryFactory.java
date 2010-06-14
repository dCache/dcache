package org.dcache.gplazma.loader;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * This class is a {@link PluginRepositoryFactory} that creates a
 * {@link PluginRepository} based on the list of plugin classes passed to the
 * constructor. It is meant mainly as demonstration code and for
 * unit-testing.
 * <p>
 * The plugin name is the simple name of the class implementing this plugin,
 * so a GPlazma plugin class <tt>org.example.foo.BarPlugin</tt> is registered
 * with the name <tt>BarPlugin</tt>.
 */
public class StaticClassPluginRepositoryFactory implements
        PluginRepositoryFactory {

    private Class<? extends GPlazmaPlugin> _pluginClasses[];

    public StaticClassPluginRepositoryFactory(Class<? extends GPlazmaPlugin>... pluginClasses) {
        _pluginClasses = pluginClasses;

    }

    @Override
    public PluginRepository newRepository() {
        PluginRepository repository = new PluginRepository();

        for( Class<? extends GPlazmaPlugin> pluginClass : _pluginClasses) {
            PluginMetadata pluginMetadata = new PluginMetadata();

            String pluginName = getNameFromClass( pluginClass);
            pluginMetadata.addName( pluginName);
            pluginMetadata.setPluginClass( pluginClass);

            repository.addPlugin( pluginMetadata);
        }

        return repository;
    }

    protected static String getNameFromClass(Class<? extends GPlazmaPlugin> pluginClass) {
        return pluginClass.getSimpleName();
    }
}
