package gplazma.authz.util;

import java.io.Serializable;

/** Performs an LDAP  search.
 *
 * Holds a subjectDN and FQAN from a user's credentials, so the combination can be used as a key to a Map.
 */


public class NameRolePair implements Serializable {
    private static final long serialVersionUID = -4028540282861842633L;

    private String name;
    private String role;

    public NameRolePair(String subjectDN, String FQAN) {
        name = subjectDN;
        role = FQAN;
    }

    public String getName()  { return name; }
    public String getRole() { return role; }
    public void setName(String arg)  { name = arg; }
    public void setRole(String arg) { role = arg; }

    @Override
    public boolean equals (Object other) {
        if ( this == other ) return true;
        if ( !(other instanceof NameRolePair) ) return false;
        return name.equals(((NameRolePair) other).getName()) &&
                role.equals(((NameRolePair) other).getRole());
    }

    @Override
    public int hashCode() {
        return name.hashCode()^ role.hashCode();
    }
}
