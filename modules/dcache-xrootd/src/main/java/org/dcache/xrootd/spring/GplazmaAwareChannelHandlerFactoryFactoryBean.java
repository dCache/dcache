package org.dcache.xrootd.spring;

import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.collect.Iterables.any;

import com.google.common.collect.Lists;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.LoginStrategyAware;
import org.dcache.gsi.KeyPairCache;
import org.dcache.xrootd.core.XrootdAuthorizationHandlerFactory;
import org.dcache.xrootd.door.LoginAuthenticationHandlerFactory;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.dcache.xrootd.plugins.AuthenticationProvider;
import org.dcache.xrootd.plugins.AuthorizationFactory;
import org.dcache.xrootd.plugins.AuthorizationProvider;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.ProxyDelegationClientFactory;
import org.dcache.xrootd.security.GSIProxyDelegationClientFactory;
import org.dcache.xrootd.security.ProxyDelegationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * A Spring FactoryBean that creates ChannelHandlerFactory instances.
 * <p>
 * A ChannelHandlerFactory is created by an ChannelHandlerProvider. The FactoryBean uses the Java 6
 * ServiceLoader system to obtain ChannelHandlerProvider instances.
 */
public class GplazmaAwareChannelHandlerFactoryFactoryBean
      extends ChannelHandlerFactoryFactoryBean {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(GplazmaAwareChannelHandlerFactoryFactoryBean.class);

    private static final ServiceLoader<AuthenticationProvider> _authenticationProviders =
          ServiceLoader.load(AuthenticationProvider.class);

    private static final ServiceLoader<AuthorizationProvider> _authorizationProviders =
          ServiceLoader.load(AuthorizationProvider.class);

    private static final ServiceLoader<ProxyDelegationClientFactory> _clientFactories =
          ServiceLoader.load(ProxyDelegationClientFactory.class);

    private static final String GPLAZMA_PREFIX = "gplazma:";
    private static final String AUTHZ_PREFIX = "authz:";

    private LoginStrategy _loginStrategy;
    private LoginStrategy _anonymousLoginStrategy;
    private ProxyDelegationStore _gsiDelegationProvider;

    @Required
    public void setPlugins(String plugins) {
        super.setPlugins(plugins);

        if (any(_plugins, containsPattern("^authn:"))) {
            throw new IllegalArgumentException(
                  "The authn: prefix is not allowed in the xrootd door");
        }

        int authn = plugins.lastIndexOf("gplazma:");
        int authz = plugins.indexOf("authz:");
        if (authz > -1 && authz < authn) {
            throw new IllegalArgumentException(
                  "Authorization plugins must be placed after authentication plugins");
        }
    }

    public void shutdown() {
        if (_gsiDelegationProvider != null) {
            _gsiDelegationProvider.shutdown();
        }
    }

    @Required
    public void setLoginStrategy(LoginStrategy loginStrategy) {
        _loginStrategy = loginStrategy;
    }

    @Required
    public void setAnonymousLoginStrategy(
          LoginStrategy anonymousLoginStrategy) {
        _anonymousLoginStrategy = anonymousLoginStrategy;
    }

    @Override
    public List<ChannelHandlerFactory> getObject()
          throws Exception {
        List<ChannelHandlerFactory> factories = Lists.newArrayList();
        for (String plugin : _plugins) {
            if (plugin.startsWith(GPLAZMA_PREFIX)) {
                String name = plugin.substring(GPLAZMA_PREFIX.length());
                factories.add(createAuthenticationHandlerFactory(name));
            } else if (plugin.startsWith(AUTHZ_PREFIX)) {
                String name = plugin.substring(AUTHZ_PREFIX.length());
                factories.add(createAuthorizationHandlerFactory(name));
            } else {
                factories.add(createChannelHandlerFactory(plugin));
            }
        }
        return factories;
    }

    private ChannelHandlerFactory createAuthenticationHandlerFactory(String name) throws Exception {
        if (name.equals("none")) {
            /*
             * The substitution of 'unix' for 'none' matches the NoAuthenticationHandler
             * protocol sent to the xrootd client.  That handler does not, however, process the
             * uid:gid credential returned, but treats this as an anonymous login.
             * In order to chain this as a last resort with other protocols, such a strategy
             * is necessary.   This use of 'unix' is for under the covers, so we still
             * map the handler to 'gplazma:none' in the dCache property configuration.
             */
            return new LoginAuthenticationHandlerFactory(GPLAZMA_PREFIX + "unix",
                  _anonymousLoginStrategy);
        }

        for (AuthenticationProvider provider : _authenticationProviders) {
            AuthenticationFactory authnFactory = provider.createFactory(name, _properties);
            if (authnFactory != null) {
                ProxyDelegationClientFactory clientFactory
                      = createProxyDelegationClientFactory(name);
                return new LoginAuthenticationHandlerFactory(GPLAZMA_PREFIX + name,
                      name,
                      clientFactory,
                      _properties,
                      authnFactory,
                      _loginStrategy);
            }
        }

        throw new IllegalArgumentException("Authentication plugin not found: " + name);
    }

    private ChannelHandlerFactory createAuthorizationHandlerFactory(String name) throws Exception {
        for (AuthorizationProvider provider : _authorizationProviders) {
            AuthorizationFactory authzFactory = provider.createFactory(name, _properties);
            if (authzFactory != null) {
                if (authzFactory instanceof LoginStrategyAware) {
                    ((LoginStrategyAware) authzFactory).setLoginStrategy(_loginStrategy);
                }
                return new XrootdAuthorizationHandlerFactory(authzFactory);
            }
        }

        throw new IllegalArgumentException("Authorization plugin not found: " + name);
    }

    private ProxyDelegationClientFactory createProxyDelegationClientFactory(String name) throws FileNotFoundException {
        for (ProxyDelegationClientFactory factory : _clientFactories) {
            if (factory instanceof GSIProxyDelegationClientFactory) {
                ((GSIProxyDelegationClientFactory) factory)
                      .setProvider(getGsiProxyDelegationProvider());
            }

            if (factory.createClient(name, _properties) != null) {
                return factory;
            }
        }

        LOGGER.debug("No delegation client for {}.", name);
        return null;
    }

    private ProxyDelegationStore getGsiProxyDelegationProvider() throws FileNotFoundException {
        LOGGER.debug("get ProxyDelegationProvider called");

        /*
         *  Should be singleton.
         */
        if (_gsiDelegationProvider != null) {
            return _gsiDelegationProvider;
        }

        LOGGER.debug("constructing and initializing ProxyDelegationStore");

        String caPath = _properties.getProperty("xrootd.gsi.ca.path");
        Long refresh = Long.valueOf(_properties.getProperty("xrootd.gsi.ca.refresh"));
        TimeUnit refreshUnit = TimeUnit.valueOf(
              _properties.getProperty("xrootd.gsi.ca.refresh.unit"));
        String vomsDir = _properties.getProperty("xrootd.gsi.vomsdir.dir");
        _gsiDelegationProvider = new ProxyDelegationStore();
        _gsiDelegationProvider.setCaCertificatePath(caPath);
        _gsiDelegationProvider.setTrustAnchorRefreshInterval(refresh);
        _gsiDelegationProvider.setTrustAnchorRefreshIntervalUnit(refreshUnit);
        _gsiDelegationProvider.setVomsDir(vomsDir);
        _gsiDelegationProvider.setKeyPairCache(new KeyPairCache(1, TimeUnit.MINUTES));
        _gsiDelegationProvider.initialize();

        return _gsiDelegationProvider;
    }
}
