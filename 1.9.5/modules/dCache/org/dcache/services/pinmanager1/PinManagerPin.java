/*
 * PinManagerPin.java
 *
 * Created on August 9, 2007, 3:34 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.services.pinmanager1;
import diskCacheV111.util.PnfsId;

/**
 *
 * @author timur
 */
public class PinManagerPin {

    private long id;
    private PnfsId pnfsId;
    private String pool;
    private long creationTime;
    private long expirationTime;
    private long srmRequestId;
    private PinManagerPinState state;


    /** Creates a new instance of PinManagerPin */
    public PinManagerPin(long id,
     PnfsId pnfsId,
     String pool,
     long creationTime,
     long expirationTime,
     long srmRequestId,
     PinManagerPinState state ) {
        this.id = id;
        this.pnfsId = pnfsId;
        this.pool = pool;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.srmRequestId = srmRequestId;
        this.state = state;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public void setPnfsId(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
    }

    public String getPool() {
        return pool;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getSrmRequestId() {
        return srmRequestId;
    }

    public void setSrmRequestId(long srmRequestId) {
        this.srmRequestId = srmRequestId;
    }

    public String toString(){
        StringBuffer sb = new StringBuffer();
        toStringBuffer(sb);
        return sb.toString();
    }

    public void toStringBuffer(StringBuffer sb){
        sb.append(id).append(' ');
        sb.append("PnfsId:").append(pnfsId).append(' ');
        sb.append("pool:").append(pool).append(' ');
        sb.append("creationTime:").append(creationTime).append(' ');
        sb.append("expirationTime:").append(expirationTime).append(' ');
        sb.append("state:").append(state).append(' ');
        sb.append("srmRequestId:").append(srmRequestId);
    }
}
