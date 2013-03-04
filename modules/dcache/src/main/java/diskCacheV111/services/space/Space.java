/*
 * Space.java
 *
 * Created on July 18, 2006, 1:26 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space;

import java.io.Serializable;
import java.util.Date;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;


/**
 *
 * @author timur
 */
public class Space implements Serializable {
    private static final long serialVersionUID = -1935368561781812540L;
    private long id;
    private String voGroup;
    private String voRole;
    private RetentionPolicy retentionPolicy;
    private AccessLatency accessLatency;
    private long linkGroupId;
    private long sizeInBytes;
    private long usedSizeInBytes;
    private long allocatedSpaceInBytes;
    private long creationTime;
    private long lifetime;
    private String description;
    private SpaceState state;
    /** Creates a new instance of Space */

    public Space() {
    }

    public Space(
            long id,
            String voGroup,
            String voRole,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            long linkGroupId,
            long sizeInBytes,
            long creationTime,
            long lifetime,
            String description,
            SpaceState state,
	    long used,
	    long allocated
            ) {
        this.setId(id);
        this.voGroup = voGroup;
        this.voRole = voRole;
        this.retentionPolicy = retentionPolicy;
        this.accessLatency = accessLatency;
        this.setLinkGroupId(linkGroupId);
        this.setSizeInBytes(sizeInBytes);
        this.setCreationTime(creationTime);
        this.setLifetime(lifetime);
        this.setDescription(description);
        this.setState(state);
        this.setUsedSizeInBytes(used);
        this.setAllocatedSpaceInBytes(allocated);
    }

	// for backward compatibility

    public Space(
            long id,
            String voGroup,
            String voRole,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            long linkGroupId,
            long sizeInBytes,
            long creationTime,
            long lifetime,
            String description,
            SpaceState state)
             {
        this.setId(id);
        this.voGroup = voGroup;
        this.voRole = voRole;
        this.retentionPolicy = retentionPolicy;
        this.accessLatency = accessLatency;
        this.setLinkGroupId(linkGroupId);
        this.setSizeInBytes(sizeInBytes);
        this.setCreationTime(creationTime);
        this.setLifetime(lifetime);
        this.setDescription(description);
        this.setState(state);
        this.setUsedSizeInBytes(0L);
        this.setAllocatedSpaceInBytes(0L);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    public long getLinkGroupId() {
        return linkGroupId;
    }

    public void setLinkGroupId(long linkGroupId) {
        this.linkGroupId = linkGroupId;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public void setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getLifetime() {
        return lifetime;
    }

    public void setLifetime(long lifetime) {
        this.lifetime = lifetime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public SpaceState getState() {
        return state;
    }

    public void setState(SpaceState state) {
        this.state = state;
    }
    public String toString() {
        StringBuffer sb = new StringBuffer();
        toStringBuffer(sb);
        return sb.toString();
    }

    public void toStringBuffer(StringBuffer sb) {
                sb.append(id).append(' ');
                sb.append("voGroup:").append(voGroup).append(' ');
                sb.append("voRole:").append(voRole).append(' ');
		sb.append("retentionPolicy:").append(retentionPolicy.toString()).append(' ');
		sb.append("accessLatency:").append(accessLatency.toString()).append(' ');
                sb.append("linkGroupId:").append(linkGroupId).append(' ');
                sb.append("size:").append(sizeInBytes).append(' ');
                sb.append("created:").append((new Date(creationTime))).append(' ');
                sb.append("lifetime:").append(lifetime).append("ms ");
                sb.append("expiration:").append(lifetime==-1?"NEVER":new Date(creationTime+lifetime).toString()).append(' ');
                sb.append("description:").append(description).append(' ');
                sb.append("state:").append(state).append(' ');
                sb.append("used:").append(usedSizeInBytes).append(' ');
                sb.append("allocated:").append(allocatedSpaceInBytes).append(' ');
    }

    public String getVoGroup() {
        return voGroup;
    }

    public void setVoGroup(String voGroup) {
        this.voGroup = voGroup;
    }

    public String getVoRole() {
        return voRole;
    }

    public void setVoRole(String voRole) {
        this.voRole = voRole;
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

    public long getUsedSizeInBytes() {
        return usedSizeInBytes;
    }

    public void setUsedSizeInBytes(long usedSizeInBytes) {
        this.usedSizeInBytes = usedSizeInBytes;
    }


    public long getAllocatedSpaceInBytes() {
        return allocatedSpaceInBytes;
    }

	public long getAvailableSpaceInBytes() {
		return sizeInBytes-usedSizeInBytes-allocatedSpaceInBytes;
	}

	public void setAllocatedSpaceInBytes(long allocated) {
		this.allocatedSpaceInBytes = allocated;

    }
    /*
     * @return expriation time as long in ms ; -1 means 'Never'
     */
    public long getExpirationTime() {
        return lifetime == -1 ? -1 : creationTime + lifetime;
    }
}
