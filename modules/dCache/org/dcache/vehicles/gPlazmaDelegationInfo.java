package org.dcache.vehicles;

import java.io.Serializable;

/**
 * gPlazmaDelegationInfo.java
 * User: tdh
 * Date: Sep 16, 2008
 * Time: 10:24:46 AM
 */
public class gPlazmaDelegationInfo implements Serializable
{
    static final long serialVersionUID = 4259453276083475709L;

    private long id;
    private String user=null;
    private Long requestCredentialId;

    public gPlazmaDelegationInfo(long id, String user, Long requestCredentialId) {
        this.id=id;
        this.user=user;
        this.requestCredentialId=requestCredentialId;
    }

    /** Getter for property id.
     * @return Value of property id.
     */
    public long getId() {
        return id;
    }

    public String getUser() {
        return user;
    }

    public Long getRequestCredentialId() {
        return requestCredentialId;
    }
}
