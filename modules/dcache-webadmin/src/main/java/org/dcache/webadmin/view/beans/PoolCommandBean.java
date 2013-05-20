package org.dcache.webadmin.view.beans;

import java.io.Serializable;

/**
 * Necessary Fields for a Pool on the PoolAdminPage
 * @author jans
 */
public class PoolCommandBean implements Comparable<PoolCommandBean>, Serializable {

    private static final long serialVersionUID = -3798063889135925372L;
    private String _name = "";
    private String _domain = "";
    private String _response = "";

    public String getDomain() {
        return _domain;
    }

    public void setDomain(String domain) {
        _domain = domain;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public String getResponse() {
        return _response;
    }

    public void setResponse(String response) {
        _response = response;
    }

    @Override
    public int hashCode() {
        return getName().hashCode() ^ getDomain().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof PoolCommandBean)) {
            return false;
        }
        PoolCommandBean otherBean = (PoolCommandBean) other;

        return (getName().equals(otherBean.getName()) &&
                getDomain().equals(otherBean.getDomain()));
    }

    @Override
    public int compareTo(PoolCommandBean other) {
        return (getName().compareTo(other.getName()) +
                getDomain().compareTo(other.getDomain()));
    }
}
