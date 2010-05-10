package dmg.security;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

public class CellUser implements Principal, java.io.Serializable {

    private static final long serialVersionUID = 7437573242083470794L;

    private String _name = null;
    private final List<String> _roles = new ArrayList<String>();

    public CellUser(String name, List<String> roles) {
        _name = name;
        _roles.addAll(roles);
    }

    public String getName() {
        return _name;
    }

    public void setName(String newName) {
        _name = newName;
    }

    public List<String> getRoles() {
        return _roles;
    }

    /**
     * add new roles.
     * @param newRoles
     */
    public void setRoles(List<String> newRoles) {
        assert newRoles != null;
        _roles.addAll(newRoles);
    }

    @Override
    public String toString() {
        return _name + ":" + _roles ;
    }

}
