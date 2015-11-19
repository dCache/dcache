package org.dcache.xrootd.spring;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Required;

import java.util.List;
import java.util.ServiceLoader;

import org.dcache.auth.LoginStrategy;
import org.dcache.xrootd.door.LoginAuthenticationHandlerFactory;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.dcache.xrootd.plugins.AuthenticationProvider;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.authn.none.NoAuthenticationFactory;

import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.collect.Iterables.*;

/**
 * A Spring FactoryBean that creates ChannelHandlerFactory instances.
 *
 * A ChannelHandlerFactory is created by an ChannelHandlerProvider.
 * The FactoryBean uses the Java 6 ServiceLoader system to obtain
 * ChannelHandlerProvider instances.
 */
public class GplazmaAwareChannelHandlerFactoryFactoryBean
    extends ChannelHandlerFactoryFactoryBean
{
    private static final ServiceLoader<AuthenticationProvider> _authenticationProviders =
            ServiceLoader.load(AuthenticationProvider.class);

    private static final String GPLAZMA_PREFIX = "gplazma:";

    private LoginStrategy _loginStrategy;
    private LoginStrategy _anonymousLoginStrategy;

    @Required
    public void setPlugins(String plugins)
    {
        super.setPlugins(plugins);

        if (any(_plugins, containsPattern("^authn:"))) {
            throw new IllegalArgumentException("The authn: prefix is not allowed in the xrootd door");
        }

        if (size(filter(_plugins, containsPattern("^gplazma:"))) != 1) {
            throw new IllegalArgumentException("Exactly one authentication plugin is required");
        }

        int authn = indexOf(_plugins, containsPattern("^gplazma:"));
        int authz = indexOf(_plugins, containsPattern("^authz:"));
        if (authz > -1 && authz < authn) {
            throw new IllegalArgumentException("Authorization plugins must be placed after authentication plugins");
        }
    }

    @Required
    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        _loginStrategy = loginStrategy;
    }

    @Required
    public void setAnonymousLoginStrategy(
            LoginStrategy anonymousLoginStrategy)
    {
        _anonymousLoginStrategy = anonymousLoginStrategy;
    }

    @Override
    public List<ChannelHandlerFactory> getObject()
        throws Exception
    {
        List<ChannelHandlerFactory> factories = Lists.newArrayList();
        for (String plugin: _plugins) {
            /* We need special logic for the authentication handler as we
             * cannot use a generic provider: The provider would not have
             * access to the login strategies. REVISIT: Is there some way
             * we could get Spring to inject them anyway?
             */
            if (plugin.startsWith(GPLAZMA_PREFIX)) {
                String name = plugin.substring(GPLAZMA_PREFIX.length());
                factories.add(createAuthenticationHandlerFactory(name));
            } else {
                factories.add(createChannelHandlerFactory(plugin));
            }
        }
        return factories;
    }

    private ChannelHandlerFactory createAuthenticationHandlerFactory(
            String name) throws Exception
    {
        if (name.equals("none")) {
            return new LoginAuthenticationHandlerFactory(
                    GPLAZMA_PREFIX + "none", new NoAuthenticationFactory(), _anonymousLoginStrategy);
        }

        for (AuthenticationProvider provider: _authenticationProviders) {
            AuthenticationFactory factory = provider.createFactory(name, _properties);
            if (factory != null) {
                return new LoginAuthenticationHandlerFactory(
                        GPLAZMA_PREFIX + name, factory, _loginStrategy);
            }
        }
        throw new IllegalArgumentException("Authentication plugin not found: " + name);
    }
}
