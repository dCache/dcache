package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Set;

import diskCacheV111.util.CacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.LoginAttributes;
import org.dcache.util.CertificateFactories;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.view.beans.UserBean;

import static java.util.Arrays.asList;

/**
 *
 * @author jans
 */
public class LoginStrategyLogInService implements LogInService {

    private static final Logger _log = LoggerFactory.getLogger(LogInService.class);
    private LoginStrategy _loginStrategy;
    private final CertificateFactory _cf;

    public LoginStrategyLogInService()
    {
        this._cf = CertificateFactories.newX509CertificateFactory();
    }

    @Override
    public UserBean authenticate(String username, char[] password) throws LogInServiceException {
        Subject subject = new Subject();
        PasswordCredential pass =
            new PasswordCredential(username, String.valueOf(password));
        subject.getPrivateCredentials().add(pass);
        return authenticate(subject);
    }

    @Override
    public UserBean authenticate(X509Certificate[] certChain) throws LogInServiceException {
        try {
            Subject subject = new Subject();
            subject.getPublicCredentials().add(_cf.generateCertPath(asList(certChain)));
            return authenticate(subject);
        } catch (CertificateException e) {
            throw new LogInServiceException("Failed to generate X.509 certificate path: " + e.getMessage(),  e);
        }
    }

    public UserBean authenticate(Subject subject) throws LogInServiceException {
        LoginReply login;
        try {
            login = _loginStrategy.login(subject);
            if (login == null) {
                throw new NullPointerException();
            }
        } catch (CacheException ex) {
            throw new LogInServiceException(ex.getMessage(), ex);
        }
        return mapLoginToUser(login);
    }

    private UserBean mapLoginToUser(LoginReply login) {
        Set<LoginAttribute> attributes = login.getLoginAttributes();
        Subject subject = login.getSubject();

        UserBean user = new UserBean();
        user.setUsername(Subjects.getUserName(subject));
        LoginAttributes.assertedRoles(attributes).forEach(user::addRole);
        LoginAttributes.unassertedRoles(attributes).forEach(user::addInactiveRole);
        return user;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        if (loginStrategy == null) {
            throw new IllegalArgumentException();
        }
        _loginStrategy = loginStrategy;
    }
}
