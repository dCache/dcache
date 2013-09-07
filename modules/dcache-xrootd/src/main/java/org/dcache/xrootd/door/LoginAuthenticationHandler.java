package org.dcache.xrootd.door;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.util.CertificateFactories;
import org.dcache.xrootd.core.XrootdAuthenticationHandler;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.AuthenticationFactory;

import static java.util.Arrays.asList;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;

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
    private CertificateFactory _cf;

    public LoginAuthenticationHandler(AuthenticationFactory authenticationFactory, LoginStrategy loginStrategy)
    {
        super(authenticationFactory);
        _loginStrategy = loginStrategy;
        _cf = CertificateFactories.newX509CertificateFactory();
    }

    @Override
    protected Subject login(ChannelHandlerContext context, Subject subject)
        throws XrootdException
    {
        try {
            LoginReply reply = _loginStrategy.login(translateSubject(subject));
            context.sendUpstream(new LoginEvent(context.getChannel(), reply));
            return reply.getSubject();
        } catch (PermissionDeniedCacheException e) {
            _log.warn("Authorization denied for {}: {}",
                    subject, e.getMessage());
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (CacheException | CertificateException e) {
            _log.error("Authorization failed for {}: {}",
                       subject, e.getMessage());
            throw new XrootdException(kXR_ServerError, e.getMessage());
        }
    }

    private Subject translateSubject(Subject subject) throws CertificateException
    {
        if (subject == null) {
            return null;
        }
        Set<Object> publicCredentials = new HashSet<>();
        for (Object credential : subject.getPublicCredentials()) {
            if (credential instanceof X509Certificate[]) {
                publicCredentials.add(_cf.generateCertPath(asList((X509Certificate[]) credential)));
            } else {
                publicCredentials.add(credential);
            }
        }
        return new Subject(false, subject.getPrincipals(), publicCredentials, subject.getPrivateCredentials());
    }
}
