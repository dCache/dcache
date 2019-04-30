package org.dcache.xrootd.door;

import io.netty.channel.ChannelHandler;

import org.dcache.auth.LoginStrategy;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.ProxyDelegationClient;

public class LoginAuthenticationHandlerFactory implements ChannelHandlerFactory
{
    private final String                _name;
    private final LoginStrategy         _loginStrategy;
    private final AuthenticationFactory _authenticationFactory;
    private final ProxyDelegationClient _proxyDelegationClient;

    public LoginAuthenticationHandlerFactory(String name,
                                             AuthenticationFactory authenticationFactory,
                                             ProxyDelegationClient proxyDelegationClient,
                                             LoginStrategy loginStrategy)
    {
        _name = name;
        _authenticationFactory = authenticationFactory;
        _proxyDelegationClient = proxyDelegationClient;
        _loginStrategy = loginStrategy;
    }

    @Override
    public String getName()
    {
        return _name;
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
                                              _proxyDelegationClient,
                                              _loginStrategy);
    }
}
