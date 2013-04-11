package org.dcache.xrootd.spring;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.ChannelHandlerProvider;

/**
 * A Spring FactoryBean that creates ChannelHandlerFactory instances.
 *
 * A ChannelHandlerFactory is created by an ChannelHandlerProvider.
 * The FactoryBean uses the Java 6 ServiceLoader system to obtain
 * ChannelHandlerProvider instances.
 */
public class ChannelHandlerFactoryFactoryBean
        implements FactoryBean<List<ChannelHandlerFactory>>, EnvironmentAware
{
    private final static ServiceLoader<ChannelHandlerProvider> _channelHandlerProviders =
            ServiceLoader.load(ChannelHandlerProvider.class);

    protected Iterable<String> _plugins;
    protected Properties _properties = new Properties();

    @Required
    public void setPlugins(String plugins)
    {
        _plugins = Splitter.on(",").omitEmptyStrings().split(plugins);
    }

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _properties = toProperties(environment);
    }

    @Override
    public List<ChannelHandlerFactory> getObject()
        throws Exception
    {
        List<ChannelHandlerFactory> factories = Lists.newArrayList();
        for (String plugin: _plugins) {
            factories.add(createChannelHandlerFactory(plugin));
        }
        return factories;
    }

    @Override
    public Class<List> getObjectType()
    {
        return List.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }

    protected ChannelHandlerFactory createChannelHandlerFactory(String plugin)
            throws Exception
    {
        for (ChannelHandlerProvider provider: _channelHandlerProviders) {
            ChannelHandlerFactory factory =
                    provider.createFactory(plugin, _properties);
            if (factory != null) {
                return factory;
            }
        }
        throw new IllegalArgumentException("Channel handler plugin not found: " + plugin);
    }

    private static Properties toProperties(final Map<String,Object> env)
    {
        Replaceable replaceable = new Replaceable() {
            @Override
            public String getReplacement(String name)
            {
                Object value =  env.get(name);
                return (value == null) ? null : value.toString().trim();
            }
        };

        Properties properties = new Properties();
        for (Map.Entry<String,Object> e: env.entrySet()) {
            String key = e.getKey();
            String value = String.valueOf(e.getValue());
            properties.put(key, Formats.replaceKeywords(value, replaceable));
        }

        return properties;
    }
}
