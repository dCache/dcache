package org.dcache.webadmin.controller;

import java.security.cert.X509Certificate;

import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.view.beans.UserBean;

/**
 * Services for Logging-In
 * @author jans
 */
public interface LogInService {

    public UserBean authenticate(String username, char[] password) throws LogInServiceException;

    public UserBean authenticate(X509Certificate[] certChain) throws LogInServiceException;
}
