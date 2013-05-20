package org.dcache.webadmin.view.beans;

import org.apache.wicket.authroles.authorization.strategies.role.Roles;

import java.io.Serializable;

/**
 * A Representation of the User in Webadmin
 * @author jans
 */
public class UserBean implements Serializable {

    private static final long serialVersionUID = -266376958298121232L;
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
