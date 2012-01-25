package org.dcache.gplazma.util;

import java.io.Serializable;

/**
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

    /**
     *  Check whether NamePairRole has the supplied name.
     */
    public boolean hasName(String name) {
        return this.name==null ? name==null : this.name.equals(name);
    }

    /**
     *  Check whether NamePairRole has the supplied role.
     */
    public boolean hasRole(String role) {
        return this.role==null ? role==null : this.role.equals(role);
    }


    @Override
    public boolean equals (Object other) {
        if ( this == other ) return true;
        if ( !(other instanceof NameRolePair) ) return false;
        NameRolePair otherPair = (NameRolePair) other;
        return otherPair.hasName(name) && otherPair.hasRole(role);
    }

    @Override
    public int hashCode() {
        if(name==null && role==null) return 0;
        if(role==null) return name.hashCode();
        if(name==null) return role.hashCode();
        return name.hashCode()^ role.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb = (name!=null) ? sb.append(name) : sb;
        sb = (role!=null) ? sb.append(role) : sb;
        return sb.toString();
    }
}
