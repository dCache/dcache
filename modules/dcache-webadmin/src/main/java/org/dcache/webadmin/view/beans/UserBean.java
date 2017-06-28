package org.dcache.webadmin.view.beans;

import org.apache.wicket.authroles.authorization.strategies.role.Roles;

import java.io.Serializable;

/**
 * A Representation of the User in Webadmin
 * @author jans
 */
public class UserBean implements Serializable {

    private static final long serialVersionUID = -266376958298121232L;

    private final Roles _inactiveRoles = new Roles();
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

    public Roles getRoles() {
        return _roles;
    }

    public void setUsername(String username) {
        _username = username;
    }

    public boolean hasAnyRole(Roles roles) {
        return _roles.hasAnyRole(roles);
    }

    public boolean hasRole(String role) {
        return _roles.hasRole(role);
    }

    public Roles getInactiveRoles() {
        return _inactiveRoles;
    }

    public void addInactiveRole(String role) {
        _inactiveRoles.add(role);
    }

    public void activateRole(String role) {
        if (_inactiveRoles.remove(role)) {
            _roles.add(role);
        }
    }

    public void deactivateRole(String role) {
        if (_roles.remove(role)) {
            _inactiveRoles.add(role);
        }
    }

    public void activateAllRoles() {
        _roles.addAll(_inactiveRoles);
        _inactiveRoles.clear();
    }

}
