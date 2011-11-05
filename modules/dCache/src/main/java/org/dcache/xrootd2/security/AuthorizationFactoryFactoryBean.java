package org.dcache.xrootd2.security;

import java.util.ServiceLoader;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Collections;
import java.util.Map;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import static com.google.common.collect.Iterables.find;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.compose;
import com.google.common.base.Function;

/**
 * A Spring FactoryBean that creates AuthorizationFactory instances.
 *
 * An AuthorizationFactory is created by an AuthorizationProvider.
 * The FactoryBean uses the Java 6 ServiceLoader system to obtain
 * AuthorizationProvider instances.
 */
public class AuthorizationFactoryFactoryBean
    implements FactoryBean, EnvironmentAware
{
    private final static ServiceLoader<AuthorizationProvider> _providers =
        ServiceLoader.load(AuthorizationProvider.class);

    private String _name;
    private Properties _properties = new Properties();

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

    private Properties toProperties(final Map<String,Object> env)
    {
        Replaceable replaceable = new Replaceable() {
                @Override
                public String getReplacement(String name)
                {
                    Object value =  env.get(name);
                    return (value == null) ? null : value.toString();
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

    @Override
    public Object getObject()
        throws Exception
    {
        for (AuthorizationProvider provider: _providers) {
            AuthorizationFactory factory =
                provider.createFactory(_name, _properties);
            if (factory != null) {
                return factory;
            }
        }
        throw new NoSuchElementException("No such xrootd authorization plugin: " + _name);
    }

    @Override
    public Class getObjectType()
    {
        return AuthorizationFactory.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }
}