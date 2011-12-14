package org.dcache.xrootd.spring;

import java.util.ServiceLoader;
import java.util.NoSuchElementException;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.dcache.xrootd.plugins.AuthorizationProvider;
import org.dcache.xrootd.plugins.AuthorizationFactory;

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
    extends AbstractPluginFactoryBean
{
    private final static ServiceLoader<AuthorizationProvider> _providers =
        ServiceLoader.load(AuthorizationProvider.class);

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