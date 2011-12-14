package org.dcache.xrootd.door;

import javax.security.auth.Subject;

import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.CacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;

import org.dcache.xrootd.core.XrootdAuthenticationHandler;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import static org.dcache.xrootd.protocol.XrootdProtocol.*;

import org.jboss.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An XrootdAuthenticationHandler which after successful
 * authentication delegates login to a LoginStrategy.
 *
 * Generates a LoginEvent after successful login.
 */
public class LoginAuthenticationHandler
    extends XrootdAuthenticationHandler
{
    private final static Logger _log =
        LoggerFactory.getLogger(LoginAuthenticationHandler.class);

    private LoginStrategy _loginStrategy;
    private LoginStrategy _anonymousLoginStrategy;

    public LoginAuthenticationHandler(AuthenticationFactory authenticationFactory, LoginStrategy loginStrategy, LoginStrategy anonymousLoginStrategy)
    {
        super(authenticationFactory);
        _loginStrategy = loginStrategy;
        _anonymousLoginStrategy = anonymousLoginStrategy;
    }

    @Override
    protected Subject login(ChannelHandlerContext context, Subject subject)
        throws XrootdException
    {
        try {
            LoginReply reply;
            if (subject == null) {
                reply = _anonymousLoginStrategy.login(null);
            } else {
                reply = _loginStrategy.login(subject);
            }
            context.sendUpstream(new LoginEvent(context.getChannel(), reply));
            return reply.getSubject();
        } catch (PermissionDeniedCacheException e) {
            _log.warn("Authorization denied for {}: {}",
                      subject, e.getMessage());
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (CacheException e) {
            _log.error("Authorization failed for {}: {}",
                       subject, e.getMessage());
            throw new XrootdException(kXR_ServerError, e.getMessage());
        }
    }
}