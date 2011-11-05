package org.dcache.gplazma.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map.Entry;
import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

public class PropertiesPluginFactory implements PluginFactory {

    private static final Properties EMPTY_ARGUMENTS = new Properties();

    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass) {
        return newPlugin( pluginClass, EMPTY_ARGUMENTS );
    }

    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass,
            Properties properties) {
        Constructor<T> constructor = tryToGetConstructor( pluginClass);

        T plugin = tryToCreatePlugin( constructor, properties);
        return plugin;
    }

    private <T extends GPlazmaPlugin> Constructor<T> tryToGetConstructor(Class<T> pluginClass) {
        Constructor<T> constructor;

        try {
            constructor = pluginClass.getConstructor( Properties.class);
        } catch (SecurityException e) {
            throw new IllegalArgumentException("Not authorised to create plugin " +
                    pluginClass.getName(), e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Cannot create plugin as it does not accept arguments");
        }

        return constructor;
    }

    private <T extends GPlazmaPlugin> T tryToCreatePlugin(Constructor<T> constructor,
            Properties properties) {
        T plugin;

        Properties constructorProperties = new Properties();

        if (properties!=null)
            for (Entry<Object, Object> kv : properties.entrySet()) {
                constructorProperties.put(kv.getKey(), kv.getValue());
            }

        try {
            plugin = constructor.newInstance( properties );
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
            throw new IllegalArgumentException("Plugin constructor " +
                    constructor.getName() +
                    " threw exception " +
                    e.getCause(), e.getCause());
        }

        return plugin;
    }


}
