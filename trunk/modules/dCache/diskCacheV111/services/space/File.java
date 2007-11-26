/*
 * File.java
 *
 * Created on July 18, 2006, 1:39 PM
 */

package diskCacheV111.services.space;

import java.io.Serializable;
import diskCacheV111.util.PnfsId;

/**
 * @author timur
 */
public class File implements Serializable {
    private long id;
    private String voGroup;
    private String voRole;
    private long spaceId;
    private long sizeInBytes;;
    private long creationTime;
    private long lifetime;
    private String pnfsPath;
    private PnfsId pnfsId;
    private FileState state;
    public File() {
    }
    
    public File(
            long id,
            String voGroup,
            String voRole,
            long spaceId,
            long sizeInBytes,
            long creationTime,
            long lifetime,
            String pnfsPath,
            PnfsId pnfsId,
            FileState state
            ) {
        this.id = id;
        this.voGroup = voGroup;
        this.voRole = voRole;
        this.spaceId = spaceId;
        this.sizeInBytes = sizeInBytes;
        this.creationTime = creationTime;
        this.lifetime = lifetime;
        this.pnfsPath = pnfsPath;
        this.pnfsId = pnfsId;
        this.state = state;
    }
    
    public FileState getState() {
        return state;
    }
    
    public void setState(FileState state) {
        this.state = state;
    }
    
    public long getId() {
        return id;
    }
    
    public void setId(long id) {
        this.id = id;
    }
    
    
    public long getSpaceId() {
        return spaceId;
    }
    
    public void setSpaceId(long spaceId) {
        this.spaceId = spaceId;
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
    
    public String getPnfsPath() {
        return pnfsPath;
    }
    
    public void setPnfsPath(String pnfsPath) {
        this.pnfsPath = pnfsPath;
    }
    
    public PnfsId getPnfsId() {
        return pnfsId;
    }
    
    public void setPnfsId(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
    }
    public String toString() {
        return ""+ id+" "+
                voGroup+" "+
                voRole+" "+
                spaceId+" "+
                sizeInBytes+" "+
                creationTime+" "+
                lifetime+" "+
                pnfsPath+" "+
                pnfsId+" "+
                state+" ";
                
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
}
