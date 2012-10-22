package org.dcache.gplazma.loader;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * Class to hold (static) metadata about a plugin. It also checks the
 * validity of the information.
 */
public class PluginMetadata {
    private final Set<String> _names = new HashSet<>();
    private Class<? extends GPlazmaPlugin> _class;
    private String _defaultControl;

    void addName( String name) {
        validPluginNameGuard( name);
        _names.add( name);
    }

    void validPluginNameGuard( String name) {
        if( name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Plugin name cannot be null or empty.");
        }
    }

    public void setPluginClass( String className) {
        validClassNameGuard( className);
        Class<?> arbClass = tryToGetClassForName( className);
        Class<? extends GPlazmaPlugin> pluginClass = tryToCastClass( arbClass);
        setPluginClass( pluginClass);
    }

    private void validClassNameGuard( String name) {
        if( name == null) {
            throw new IllegalArgumentException("Plugin class name cannot be null");
        }

        if( hasPluginClass()) {
            throw new IllegalArgumentException("Plugin class cannot be specified twice");
        }
    }

    public boolean hasPluginClass() {
        return _class != null;
    }

    private Class<?> tryToGetClassForName( String className) {
        Class<?> arbitraryClass;

        try {
            arbitraryClass = Class.forName( className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class " + className +
                                               " could not be found.",
                                               e);
        }

        return arbitraryClass;
    }

    private Class<? extends GPlazmaPlugin> tryToCastClass( Class<?> arbClass) {
        Class<? extends GPlazmaPlugin> pluginClass;

        try {
            pluginClass = arbClass.asSubclass( GPlazmaPlugin.class);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Named plugin class is not a GPlazmaPlugin class",
                                               e);
        }

        return pluginClass;
    }

    public void setPluginClass( Class<? extends GPlazmaPlugin> pluginClass) {
        _class = pluginClass;
        addName( pluginClass.getName());
    }

    void setDefaultControl( String defaultControl) {
        _defaultControl = defaultControl;
    }

    public Set<String> getPluginNames() {
        return Collections.unmodifiableSet(_names);
    }

    public String getShortestName() {
        String shortestName = null;

        for( String name : _names) {
            if( shortestName == null || name.length() < shortestName.length()) {
                shortestName = name;
            }
        }

        return shortestName;
    }

    public Class<? extends GPlazmaPlugin> getPluginClass() {
        return _class;
    }

    public String getDefaultControl() {
        return _defaultControl;
    }

    public boolean isValid() {
        return hasPluginName() && hasPluginClass();
    }

    public boolean hasPluginName() {
        return !_names.isEmpty();
    }

}
