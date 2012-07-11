package org.dcache.webadmin.controller.impl;

import java.security.cert.X509Certificate;

import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.view.beans.UserBean;
import org.dcache.webadmin.view.util.Role;

/**
 * Adds the ability to log in as a guest and catches this case circumventing
 * the other LogInService implementations of
 * authenticate(String username, char[] password).
 * Should be the first to be called.
 * @author jans
 */
public class GuestLogInDecorator implements LogInService {

    protected LogInService _decoratedLogInService;

    public GuestLogInDecorator(LogInService decoratedLogInService) {
        _decoratedLogInService = decoratedLogInService;
    }

    @Override
    public UserBean authenticate(String username, char[] password)
            throws LogInServiceException {
        if (isGuest(username)) {
            return newGuestBean();
        }
        return _decoratedLogInService.authenticate(username, password);
    }

    private UserBean newGuestBean() {
        UserBean guest = new UserBean();
        guest.setUsername("Guest");
        guest.setRoles(new Roles(Role.GUEST));
        return guest;
    }

    private boolean isGuest(String username) {
        return (username.equalsIgnoreCase("guest"));
    }

    @Override
    public UserBean authenticate(X509Certificate[] certChain) throws LogInServiceException {
        return _decoratedLogInService.authenticate(certChain);
    }
}
