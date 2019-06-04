package org.dcache.xrootd.door;

import io.netty.channel.ChannelHandler;

import java.util.Properties;

import org.dcache.auth.LoginStrategy;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.InvalidHandlerConfigurationException;
import org.dcache.xrootd.plugins.ProxyDelegationClient;
import org.dcache.xrootd.plugins.ProxyDelegationClientFactory;
import org.dcache.xrootd.plugins.authn.none.NoAuthenticationFactory;

/**
 * Wraps the AuthenticationHandlerFactory required by the door with login
 * capabilities.  Is configured by spring initialization and SPI to create
 * an instance of the LoginAuthentciationHandler with a particular kind
 * of delegation client (if any).
 */
public class LoginAuthenticationHandlerFactory implements ChannelHandlerFactory
{
    private final String                             _authnPluginName;
    private final String                             _clientName;
    private final Properties                         _properties;
    private final LoginStrategy                      _loginStrategy;
    private final AuthenticationFactory              _authenticationFactory;
    private final ProxyDelegationClientFactory       _proxyDelegationFactory;

    /**
     * Handles logins with no authentication.
     * Properties and delegation client are unused.
     *
     * @param authnPluginName name of the authentication handler plugin
     * @param loginStrategy
     */
    public LoginAuthenticationHandlerFactory(String authnPluginName,
                                             LoginStrategy loginStrategy)
    {
        this(authnPluginName,
             null,
             null,
             null,
             new NoAuthenticationFactory(),
             loginStrategy);
    }

    /**
     * @param authnPluginName name of the authentication handler plugin
     * @param clientName name of the (type of) delegation client
     * @param proxyDelegationFactory factory to use to construct delegation client
     * @param properties passed in from the factory bean
     * @param authenticationFactory used during login
     * @param loginStrategy used for login
     */
    public LoginAuthenticationHandlerFactory(String authnPluginName,
                                             String clientName,
                                             ProxyDelegationClientFactory proxyDelegationFactory,
                                             Properties properties,
                                             AuthenticationFactory authenticationFactory,
                                             LoginStrategy loginStrategy)
    {
        _authnPluginName = authnPluginName;
        _clientName = clientName;
        _proxyDelegationFactory = proxyDelegationFactory;
        _properties = properties;
        _authenticationFactory = authenticationFactory;
        _loginStrategy = loginStrategy;
    }

    @Override
    public String getName()
    {
        return _authnPluginName;
    }

    @Override
    public String getDescription()
    {
        return "Authentication handler";
    }

    @Override
    public ChannelHandler createHandler()
    {
        return new LoginAuthenticationHandler(_authenticationFactory,
                                              createClient(),
                                              _loginStrategy);
    }

    private ProxyDelegationClient createClient()
    {
        if (_proxyDelegationFactory != null) {
            try {
                return _proxyDelegationFactory.createClient(_clientName,
                                                            _properties);
            } catch (InvalidHandlerConfigurationException e) {
                throw new IllegalArgumentException("Unable to create delegation "
                                                                   + "client "
                                                                   + _clientName
                                                                   + ": "
                                                                   + e.toString());
            }
        }
        return null;
    }
}
