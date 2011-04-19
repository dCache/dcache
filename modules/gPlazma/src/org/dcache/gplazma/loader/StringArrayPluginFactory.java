package org.dcache.gplazma.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * This class creates GPlazmaPlugin objects by calling their constructor that
 * takes a single argument: an array of Strings. When creating a plugin with
 * no arguments an empty array is passed.
 */
public class StringArrayPluginFactory implements PluginFactory {

    private static final String[] EMPTY_ARGUMENTS = new String[0];

    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass) {
        return newPlugin( pluginClass, EMPTY_ARGUMENTS);
    }

    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass,
                                                 String[] arguments) {
        Constructor<T> constructor = tryToGetConstructor( pluginClass);
        T plugin = tryToCreatePlugin( constructor, arguments);
        return plugin;
    }

    private <T extends GPlazmaPlugin> Constructor<T> tryToGetConstructor(Class<T> pluginClass) {
        Constructor<T> constructor;

        try {
            constructor = pluginClass.getConstructor( String[].class);
        } catch (SecurityException e) {
            throw new IllegalArgumentException("Not authorised to create plugin " +
                                               pluginClass.getName(), e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Cannot create plugin as it does not accept arguments");
        }

        return constructor;
    }

    private <T extends GPlazmaPlugin> T tryToCreatePlugin(Constructor<T> constructor,
                                                          String[] arguments) {
        T plugin;

        Object[] constructorArgs = new Object[] { arguments };

        try {
            plugin = constructor.newInstance( constructorArgs);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Internal error creating plugin " +
                                               constructor.getName(), e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Plugin is of a type that cannot be instantiated " +
                                               constructor.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Failed to create plugin " +
                                                constructor.getName(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Plugin constructor threw exception " +
                                               constructor.getName(), e.getCause());
        }

        return plugin;
    }
}
