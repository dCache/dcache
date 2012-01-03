package org.dcache.webadmin.view.beans;

import java.io.Serializable;
import org.apache.wicket.authorization.strategies.role.Roles;

/**
 * A Representation of the User in Webadmin
 * @author jans
 */
public class UserBean implements Serializable {

    private String _username;
    private Roles _roles = new Roles();

    public void setRoles(Roles roles) {
        _roles = roles;
    }

    public void addRole(String role) {
        _roles.add(role);
    }

    public String getUsername() {
        return _username;
    }

    public void setUsername(String username) {
        _username = username;
    }

    public boolean hasAnyRole(Roles roles) {
        return _roles.hasAnyRole(roles);
    }
}
