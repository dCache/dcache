/*
 * Reserve.java
 *
 * Created on July 20, 2006, 8:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space.message;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.Message;


/**
 *
 * @author timur
 */
public class Reserve extends Message{
    private static final long serialVersionUID = 8295404238593418916L;
    private long spaceToken;
    private long sizeInBytes;
    private RetentionPolicy retentionPolicy;
    private AccessLatency accessLatency;
    private long lifetime;
    private long expirationTime;
    private String description;
    /** Creates a new instance of Reserve */
    public Reserve() {
    }

    public Reserve(
            long sizeInBytes,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            long lifetime,
            String description){
        this.sizeInBytes = sizeInBytes;
        this.lifetime = lifetime;
        this.accessLatency = accessLatency;
        this.retentionPolicy = retentionPolicy;
        this.description = description;
        setReplyRequired(true);
    }

    public long getSpaceToken() {
        return spaceToken;
    }

    public void setSpaceToken(long spaceToken) {
        this.spaceToken = spaceToken;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public void setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public RetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    public AccessLatency getAccessLatency() {
        return accessLatency;
    }

    public void setAccessLatency(AccessLatency accessLatency) {
        this.accessLatency = accessLatency;
    }

    public long getLifetime() {
        return lifetime;
    }

    public void setLifetime(long lifetime) {
        this.lifetime = lifetime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
