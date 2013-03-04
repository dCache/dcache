package org.dcache.xrootd.door;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;

import org.dcache.auth.LoginReply;

import static org.jboss.netty.channel.Channels.succeededFuture;

public class LoginEvent implements ChannelEvent
{
    private final Channel _channel;
    private LoginReply _loginReply;

    public LoginEvent(Channel channel, LoginReply loginReply)
    {
        _channel = channel;
        _loginReply = loginReply;
    }

    public LoginReply getLoginReply()
    {
        return _loginReply;
    }

    @Override
    public Channel getChannel()
    {
        return _channel;
    }

    @Override
    public ChannelFuture getFuture()
    {
        return succeededFuture(getChannel());
    }
}
