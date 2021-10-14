package org.dcache.auth;

import java.io.Serializable;
import org.dcache.util.Strings;

/**
 * The class PasswordCredential acts as a holder for username and password.
 */
public class PasswordCredential implements Serializable {

    private static final long serialVersionUID = -8823304503972043526L;

    private final String _username;
    private final String _password;

    public PasswordCredential(String username, String password) {
        _username = username;
        _password = password;
    }

    public String getUsername() {
        return _username;
    }

    public String getPassword() {
        return _password;
    }

    public String describeCredential() {
        return "username=" + _username + ",password={Hash:" + Strings.base64Hash(_password) + "}";
    }

    @Override
    public int hashCode() {
        return _username.hashCode() ^ _password.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof PasswordCredential)) {
            return false;
        }

        PasswordCredential other = (PasswordCredential) o;
        return _username.equals(other._username) && _password.equals(other._password);
    }

    @Override
    public String toString() {
        return PasswordCredential.class.getSimpleName() + "[" + describeCredential() + "]";
    }
}
