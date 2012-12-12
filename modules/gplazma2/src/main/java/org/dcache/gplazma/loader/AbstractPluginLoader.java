package org.dcache.gplazma.loader;

import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * This abstract class contains the common code for all PluginLoader classes.
 * It supports creating fresh plugins (objects of a class that implements
 * GPlazmaPlugin) from some corresponding {@link PluginMetadata}.  The PluginMetadata
 * for a plugin is obtains from a {@link PluginRepository} which, in turn, comes
 * from an abstract {@link PluginRepositoryFactory}.
 * <p>
 * Classes that extend this class to provide a concrete PluginLoader must
 * supply the PluginRepositoryFactory object that will provide the
 * PluginRepository.
 */
public abstract class AbstractPluginLoader implements PluginLoader
{
    private PluginFactory _factory = new PropertiesPluginFactory();
    private PluginRepository _repository;

    @Override
    public void setPluginFactory(PluginFactory factory)
    {
        _factory = factory;
    }

    @Override
    public void init()
    {
        PluginRepositoryFactory repositoryFactory = getRepositoryFactory();
        _repository = repositoryFactory.newRepository();
    }

    abstract PluginRepositoryFactory getRepositoryFactory();

    @Override
    public GPlazmaPlugin newPluginByName(String name)
            throws PluginLoadingException
    {
        PluginMetadata plugin = _repository.getPlugin(name);
        return _factory.newPlugin(plugin.getPluginClass());
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name, Properties properties)
            throws PluginLoadingException
    {
        PluginMetadata plugin = _repository.getPlugin(name);
        return _factory.newPlugin(plugin.getPluginClass(), properties);
    }
}
