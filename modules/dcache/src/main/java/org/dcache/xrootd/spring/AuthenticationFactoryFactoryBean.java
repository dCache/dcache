package org.dcache.xrootd.spring;

import java.util.ServiceLoader;
import java.util.NoSuchElementException;

import org.dcache.xrootd.plugins.AuthenticationProvider;
import org.dcache.xrootd.plugins.AuthenticationFactory;

import static com.google.common.collect.Iterables.find;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.compose;
import com.google.common.base.Function;

/**
 * A Spring FactoryBean that creates AuthenticationFactory instances.
 *
 * An AuthenticationFactory is created by an AuthenticationProvider.
 * The FactoryBean uses the Java 6 ServiceLoader system to obtain
 * AuthenticationProvider instances.
 */
public class AuthenticationFactoryFactoryBean
    extends AbstractPluginFactoryBean
{
    private final static ServiceLoader<AuthenticationProvider> _providers =
        ServiceLoader.load(AuthenticationProvider.class);

    @Override
    public Object getObject()
        throws Exception
    {
        for (AuthenticationProvider provider: _providers) {
            AuthenticationFactory factory =
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
        return AuthenticationFactory.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }
}