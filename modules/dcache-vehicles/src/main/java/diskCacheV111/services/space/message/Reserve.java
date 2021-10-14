/*
 * Reserve.java
 *
 * Created on July 20, 2006, 8:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space.message;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.Message;
import javax.annotation.Nonnull;

/**
 * @author timur
 */
public class Reserve extends Message {

    private static final long serialVersionUID = 8295404238593418916L;
    private long spaceToken;
    private final long sizeInBytes;
    private final RetentionPolicy retentionPolicy;
    private final AccessLatency accessLatency;
    private final long lifetime;
    private long expirationTime;
    private final String description;
    private final String linkgroupName;

    public Reserve(
          String linkgroupName,
          long sizeInBytes,
          RetentionPolicy retentionPolicy,
          AccessLatency accessLatency,
          long lifetime,
          String description) {
        this.sizeInBytes = sizeInBytes;
        this.lifetime = lifetime;
        this.accessLatency = accessLatency;
        this.retentionPolicy = requireNonNull(retentionPolicy);
        this.description = description;
        this.linkgroupName = linkgroupName;
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

    @Nonnull
    public RetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    public AccessLatency getAccessLatency() {
        return accessLatency;
    }

    public long getLifetime() {
        return lifetime;
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

    public String getLinkgroupName() {
        return linkgroupName;
    }
}
