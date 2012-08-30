package org.dcache.xrootd.door;

import org.dcache.auth.LoginStrategy;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.jboss.netty.channel.ChannelHandler;

public class LoginAuthenticationHandlerFactory implements ChannelHandlerFactory
{
    private final String _name;
    private final LoginStrategy _loginStrategy;
    private final AuthenticationFactory _authenticationFactory;

    public LoginAuthenticationHandlerFactory(String name,
                                             AuthenticationFactory authenticationFactory,
                                             LoginStrategy loginStrategy)
    {
        _name = name;
        _authenticationFactory = authenticationFactory;
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
        return new LoginAuthenticationHandler(_authenticationFactory, _loginStrategy);
    }
}
