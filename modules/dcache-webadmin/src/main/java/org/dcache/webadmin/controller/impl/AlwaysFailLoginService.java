package org.dcache.webadmin.controller.impl;

import java.security.cert.X509Certificate;

import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.view.beans.UserBean;

/**
 * A LoginService that always fails throwing an LogInServiceException with an
 * errormessage.
 * @author jans
 */
public class AlwaysFailLoginService implements LogInService {

    private static final String ERROR_MESSAGE =
            "Authentication turned off in configuration";

    @Override
    public UserBean authenticate(String username, char[] password)
            throws LogInServiceException {
        throw new LogInServiceException(ERROR_MESSAGE);
    }

    @Override
    public UserBean authenticate(X509Certificate[] certChain)
            throws LogInServiceException {
        throw new LogInServiceException(ERROR_MESSAGE);
    }
}
