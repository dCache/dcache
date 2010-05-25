package org.dcache.gplazma.loader;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * The PluginLoader interface describes how a PluginLoader should be
 * initialised precisely once and that this must happen before any calls to
 * getPluginByName.
 * <p>
 * This decorator wraps some existing PluginLoader and enforces this behaviour.
 */
public class SafePluginLoaderDecorator implements PluginLoader {
    private boolean _haveInitialised;
    private PluginLoader _inner;

    public SafePluginLoaderDecorator( PluginLoader inner) {
        _inner = inner;
    }

    @Override
    public void init() {
        alreadyInitGuard();
        _inner.init();
        _haveInitialised = true;
    }

    private void alreadyInitGuard() {
        if( _haveInitialised) {
            throw new IllegalStateException("Cannot call init twice");
        }
    }

    @Override
    public GPlazmaPlugin newPluginByName( String name) {
        notInitialisedGuard();
        return _inner.newPluginByName( name);
    }

    @Override
    public GPlazmaPlugin newPluginByName(String name, String[] arguments) {
        notInitialisedGuard();
        return _inner.newPluginByName(name, arguments);
    }

    private void notInitialisedGuard() {
        if( !_haveInitialised) {
            throw new IllegalStateException("PluginLoader has not been initialised");
        }
    }
}
