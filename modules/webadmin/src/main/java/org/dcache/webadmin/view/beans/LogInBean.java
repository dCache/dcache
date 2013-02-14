package org.dcache.webadmin.view.beans;

import java.io.Serializable;

/**
 * Holds the data of the LoginPage
 * @author jans
 */
public class LogInBean implements Serializable {

    private static final long serialVersionUID = -4692746509263501015L;
    private String _username = "";
    private String _password = "";
    private boolean _remembering = true;

    public String getPassword() {
        return _password;
    }

    public void setPassword(String password) {
        _password = password;
    }

    public boolean isRemembering() {
        return _remembering;
    }

    public void setRemembering(boolean rememberMe) {
        _remembering = rememberMe;
    }

    public String getUsername() {
        return _username;
    }

    public void setUsername(String username) {
        _username = username;
    }
}
