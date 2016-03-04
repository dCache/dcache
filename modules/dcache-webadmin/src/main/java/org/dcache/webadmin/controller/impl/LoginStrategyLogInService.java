package org.dcache.webadmin.controller.impl;

import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import diskCacheV111.util.CacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.util.CertificateFactories;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.view.beans.UserBean;
import org.dcache.webadmin.view.util.Role;

import static java.util.Arrays.asList;

/**
 *
 * @author jans
 */
public class LoginStrategyLogInService implements LogInService {

    private static final Logger _log = LoggerFactory.getLogger(LogInService.class);
    private LoginStrategy _loginStrategy;
    private int _adminGid;
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
        UserBean user = new UserBean();
        Subject subject = login.getSubject();
        user.setUsername(Subjects.getUserName(subject));
        Roles roles = mapGidsToRoles(Subjects.getGids(subject));
        user.setRoles(roles);
        return user;
    }

    private Roles mapGidsToRoles(long[] gids) {
        Roles roles = new Roles();
        boolean isAdmin = false;
        for (long gid : gids) {
            _log.debug("GID : {}", gid);
            if (gid == _adminGid) {
                roles.add(Role.ADMIN);
                isAdmin = true;
            }
        }
        if (!isAdmin) {
            roles.add(Role.USER);
        }
        return roles;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        if (loginStrategy == null) {
            throw new IllegalArgumentException();
        }
        _loginStrategy = loginStrategy;
    }

    public void setAdminGid(int adminGid) {
        _log.debug("admin GID set to {}", adminGid);
        _adminGid = adminGid;
    }
}
