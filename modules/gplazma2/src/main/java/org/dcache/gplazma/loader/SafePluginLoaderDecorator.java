package org.dcache.gplazma.loader;

import static com.google.common.base.Preconditions.checkState;

import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * The PluginLoader interface describes how a PluginLoader should be
 * initialised precisely once and that this must happen before any calls to
 * getPluginByName.
 * <p>
 * This decorator wraps some existing PluginLoader and enforces this behaviour.
 */
public class SafePluginLoaderDecorator implements PluginLoader
{
    private boolean _haveInitialised;
    private final PluginLoader _inner;

    public SafePluginLoaderDecorator(PluginLoader inner)
    {
        _inner = inner;
    }

    @Override
    public void init()
    {
        checkState(!_haveInitialised, "Cannot call init twice");
        _inner.init();
        _haveInitialised = true;
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name)
            throws PluginLoadingException
    {
        checkState(_haveInitialised, "PluginLoader has not been initialised");
        return _inner.newPluginByName(name);
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name, Properties properties)
            throws PluginLoadingException
    {
        checkState(_haveInitialised, "PluginLoader has not been initialised");
        return _inner.newPluginByName(name, properties);
    }
}
