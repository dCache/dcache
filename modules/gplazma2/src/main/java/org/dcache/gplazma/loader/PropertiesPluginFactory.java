package org.dcache.gplazma.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map.Entry;
import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * This class creates a plugin by reflection when given the plugin Class.
 * The plugin must have a constructor that accepts a single argument: a
 * Properties object.  If the newPlugin method is used that takes only
 * a Class then the plugin is created with an empty Properties object.
 */
public class PropertiesPluginFactory implements PluginFactory
{
    private static final Properties EMPTY_ARGUMENTS = new Properties();


    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass)
            throws PluginLoadingException
    {
        return newPlugin(pluginClass, EMPTY_ARGUMENTS);
    }


    @Override
    public <T extends GPlazmaPlugin> T newPlugin(Class<T> pluginClass,
            Properties properties) throws PluginLoadingException
    {
        Constructor<T> constructor = tryToGetConstructor(pluginClass);
        return tryToCreatePlugin(constructor, properties);
    }


    private <T extends GPlazmaPlugin> Constructor<T> tryToGetConstructor(Class<T> pluginClass)
            throws PluginLoadingException
    {
        Constructor<T> constructor;

        try {
            constructor = pluginClass.getConstructor(Properties.class);
        } catch (SecurityException e) {
            throw new PluginLoadingException("not authorised", e);
        } catch (NoSuchMethodException e) {
            throw new PluginLoadingException("constructor missing", e);
        }

        return constructor;
    }


    private <T extends GPlazmaPlugin> T tryToCreatePlugin(Constructor<T>
            constructor, Properties properties) throws PluginLoadingException
    {
        T plugin;

        Properties constructorProperties = new Properties();

        if (properties!=null) {
            for (Entry<Object, Object> kv : properties.entrySet()) {
                constructorProperties.put(kv.getKey(), kv.getValue());
            }
        }

        try {
            plugin = constructor.newInstance(properties);
        } catch (IllegalArgumentException e) {
            throw new PluginLoadingException("missing correct constructor", e);
        } catch (InstantiationException e) {
            throw new PluginLoadingException("type cannot be instantiated", e);
        } catch (IllegalAccessException e) {
            throw new PluginLoadingException("unauthorised", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            throw new PluginLoadingException(cause.getMessage(), cause);
        }

        return plugin;
    }
}
