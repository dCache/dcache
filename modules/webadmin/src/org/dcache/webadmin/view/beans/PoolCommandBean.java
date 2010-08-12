package org.dcache.webadmin.view.beans;

import java.io.Serializable;

/**
 * Necessary Fields for a Pool on the PoolAdminPage
 * @author jans
 */
public class PoolCommandBean implements Comparable<PoolCommandBean>, Serializable {

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
    public int compareTo(PoolCommandBean other) {
        if (other == null) {
            throw new NullPointerException();
        }

        return this.getName().compareTo(other.getName());

    }
}
