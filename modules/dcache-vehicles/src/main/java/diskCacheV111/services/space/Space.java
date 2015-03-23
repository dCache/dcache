package diskCacheV111.services.space;

import java.io.Serializable;
import java.util.Date;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

public class Space implements Serializable {
    private static final long serialVersionUID = -1935368561781812540L;
    private final long id;
    private String voGroup;
    private String voRole;
    private RetentionPolicy retentionPolicy;
    private AccessLatency accessLatency;
    private long linkGroupId;
    private long sizeInBytes;
    private long usedSizeInBytes;
    private long allocatedSpaceInBytes;
    private long creationTime;
    private Long expirationTime;
    private String description;
    private SpaceState state;

    public Space(
            long id,
            String voGroup,
            String voRole,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            long linkGroupId,
            long sizeInBytes,
            long creationTime,
            Long expirationTime,
            String description,
            SpaceState state,
	        long used,
            long allocated)
    {
        this.id = id;
        this.voGroup = voGroup;
        this.voRole = voRole;
        this.retentionPolicy = retentionPolicy;
        this.accessLatency = accessLatency;
        this.linkGroupId = linkGroupId;
        this.sizeInBytes = sizeInBytes;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.description = description;
        this.state = state;
        this.usedSizeInBytes = used;
        this.allocatedSpaceInBytes = allocated;
    }

    public long getId() {
        return id;
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
        long usedSpace = getUsedSizeInBytes() + getAllocatedSpaceInBytes();
        if (sizeInBytes < usedSpace) {
            throw new IllegalStateException(
                    "Cannot downsize space reservation below " + usedSpace + " bytes, release files first.");
        }
        this.sizeInBytes = sizeInBytes;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
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
        if (this.state.isFinal()) {
            throw new IllegalStateException(
                    "Change from " + this.state + " to " + state + " is not allowed.");
        }
        this.state = state;
    }
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(' ');
        sb.append("voGroup:").append(voGroup).append(' ');
        sb.append("voRole:").append(voRole).append(' ');
        sb.append("retentionPolicy:").append(retentionPolicy.toString()).append(' ');
        sb.append("accessLatency:").append(accessLatency.toString()).append(' ');
        sb.append("linkGroupId:").append(linkGroupId).append(' ');
        sb.append("size:").append(sizeInBytes).append(' ');
        sb.append("created:").append((new Date(creationTime))).append(' ');
        if (expirationTime != null) {
            sb.append("expiration:").append(new Date(expirationTime).toString()).append(' ');
        }
        sb.append("description:").append(description).append(' ');
        sb.append("state:").append(state).append(' ');
        sb.append("used:").append(usedSizeInBytes).append(' ');
        sb.append("allocated:").append(allocatedSpaceInBytes).append(' ');
        return sb.toString();
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

    public long getAllocatedSpaceInBytes() {
        return allocatedSpaceInBytes;
    }

	public long getAvailableSpaceInBytes() {
		return sizeInBytes-usedSizeInBytes-allocatedSpaceInBytes;
	}

    public Long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(Long expirationTime)
    {
        this.expirationTime = expirationTime;
    }
}
