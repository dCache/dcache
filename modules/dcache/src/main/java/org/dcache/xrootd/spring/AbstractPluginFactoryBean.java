package org.dcache.xrootd.spring;

import java.util.Properties;
import java.util.Map;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Base class for EnvironmentAware Spring FactoryBeans.
 */
public abstract class AbstractPluginFactoryBean
    implements FactoryBean, EnvironmentAware
{
    protected String _name;
    protected Properties _properties = new Properties();

    @Required
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _properties = toProperties(environment);
    }

    protected static Properties toProperties(final Map<String,Object> env)
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