/*
 * Pin.java
 *
 * Created on December 17, 2007, 5:13 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.services.pinmanager1;
import diskCacheV111.util.PnfsId;
import java.util.Set;
import java.util.Date;
import java.util.Collections;

/**
 *
 * @author timur
 */
public class Pin {
    private long id;
    private PnfsId pnfsId;
    private long creationTime;
    private long expirationTime;
    private  long stateTransitionTime;
    private String pool;
    private PinManagerPinState state;
    private transient Set<PinRequest> requests = Collections.emptySet();

    /** Creates a new instance of Pin */
    public Pin(long id,
        PnfsId pnfsId,
        long creationTime,
        long expirationTime,
        String pool,
        long stateTransitionTime,
        PinManagerPinState state) {
        this.id = id;
        this.pnfsId = pnfsId;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.pool = pool;
        this.setStateTransitionTime(stateTransitionTime);
        this.state = state;
    }

    public long getId() {
        return id;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }


    public long getCreationTime() {
        return creationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public String getPool() {
        return pool;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }

    public PinManagerPinState getState() {
        return state;
    }

    public void setState(PinManagerPinState state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return ""+ id+
            " PnfsId:"+pnfsId+
            " created:"+ new Date(creationTime).toString()+" "+
            " expires:"+ (expirationTime==-1?"Never":new Date(expirationTime).toString())+
            " pool:"+pool+
            " stateChangedAt:"+ new Date(stateTransitionTime).toString()+
            " state:"+ state+" ";

    }

    public long getStateTransitionTime() {
        return stateTransitionTime;
    }

    public void setStateTransitionTime(long stateTransitionTime) {
        this.stateTransitionTime = stateTransitionTime;
    }

    public Set<PinRequest> getRequests() {
        return requests;
    }

    /**
     * @param requests requests in this pin, could be empty, should never be null
     */
    public void setRequests(Set<PinRequest> requests) {
        assert requests != null;
        this.requests = requests;
    }

    public int getRequestsNum() {
        return requests.size();
    }


}
