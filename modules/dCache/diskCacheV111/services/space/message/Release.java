/*
 * Reserve.java
 *
 * Created on July 20, 2006, 8:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space.message;
import diskCacheV111.vehicles.Message;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.AccessLatency;


/**
 *
 * @author timur
 */
public class Release extends Message{
    static final long serialVersionUID = 5429671420465560457L;
    private long spaceToken;
    private Long releaseSizeInBytes; // all if null
    private long remainingSizeInBytes;
    private String voGroup=null;
    private String voRole=null;

    /** Creates a new instance of Reserve */
    public Release() {
    }
    
    public Release(
            long spaceToken,
            Long releaseSizeInBytes){
        this.spaceToken = spaceToken;
        this.releaseSizeInBytes = releaseSizeInBytes;
        setReplyRequired(true);
    }
    
    public Release(
            long spaceToken,
            Long releaseSizeInBytes, 
            String group,
            String role) {
        this.spaceToken = spaceToken;
        this.releaseSizeInBytes = releaseSizeInBytes;
        this.voGroup=group;
        this.voRole=role;
        setReplyRequired(true);
    }

    public void setVoGroup(String group) { 
        voGroup=group;
    }

    public void setVoRole(String role) { 
        voRole=role;
    }

    public String getVoGroup() { return voGroup; }

    public String getVoRole() { return voRole; }

    public long getSpaceToken() {
        return spaceToken;
    }

    public void setSpaceToken(long spaceToken) {
        this.spaceToken = spaceToken;
    }

    public Long getReleaseSizeInBytes() {
        return releaseSizeInBytes;
    }

    public void setReleaseSizeInBytes(Long releaseSizeInBytes) {
        this.releaseSizeInBytes = releaseSizeInBytes;
    }

    public long getRemainingSizeInBytes() {
        return remainingSizeInBytes;
    }

    public void setRemainingSizeInBytes(long remainingSizeInBytes) {
        this.remainingSizeInBytes = remainingSizeInBytes;
    }

}
