/*
 * SpaceManagerReserveSpaceMessage.java
 *
 * Created on February 1, 2005, 12:59 PM
 */

package diskCacheV111.vehicles.spaceManager;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsId;

/**
 *
 * @author  timur
 */
public class SpaceManagerGetInfoAndLockReservationMessage extends SpaceManagerMessage{
    //request parameters
    protected String path;
    protected long availableLockedSize;
    protected String pool;
    protected String hostToTransferIntoSpace;
    protected long creation_time;
    protected long experation_time;
    
    /** Creates a new instance of SpaceManagerGetReservationInfoByPathMessage */
    public SpaceManagerGetInfoAndLockReservationMessage(long spaceToken) {
        super(spaceToken);
    }
    

       /** Creates a new instance of SpaceManagerGetReservationInfoByPathMessage */
    public SpaceManagerGetInfoAndLockReservationMessage() {
    }
    
    public String toString(){
        String cname = this.getClass().getName();
        if(cname.lastIndexOf('.') >0) {
            cname = cname.substring(cname.lastIndexOf('.'));
        }
        return cname+
	"["+spaceToken+
            ", path="+path +
            ", availocked="+availableLockedSize+
            ", pool="+pool+
            ", hostToTransferIntoSpace="+hostToTransferIntoSpace+
            ", creation_time="+creation_time+
            ", experation_time="+experation_time+
        "]";        
        
    }

    
    /**
     * Getter for property path.
     * @return Value of property path.
     */
    public java.lang.String getPath() {
        return path;
    }
    
    /**
     * Setter for property path.
     * @param path New value of property path.
     */
    public void setPath(java.lang.String path) {
        this.path = path;
    }
    
    
    /**
     * Getter for property hostToTransferIntoSpace.
     * @return Value of property hostToTransferIntoSpace.
     */
    public java.lang.String getHostToTransferIntoSpace() {
        return hostToTransferIntoSpace;
    }
    
    /**
     * Setter for property hostToTransferIntoSpace.
     * @param hostToTransferIntoSpace New value of property hostToTransferIntoSpace.
     */
    public void setHostToTransferIntoSpace(java.lang.String hostToTransferIntoSpace) {
        this.hostToTransferIntoSpace = hostToTransferIntoSpace;
    }
    
    /** Getter for property pool.
     * @return Value of property pool.
     *
     */
    public java.lang.String getPool() {
        return pool;
    }    
   
    /** Setter for property pool.
     * @param pool New value of property pool.
     *
     */
    public void setPool(java.lang.String pool) {
        this.pool = pool;
    }
    
    /** Getter for property creation_time.
     * @return Value of property creation_time.
     *
     */
    public long getCreation_time() {
        return creation_time;
    }
    
    /** Setter for property creation_time.
     * @param creation_time New value of property creation_time.
     *
     */
    public void setCreation_time(long creation_time) {
        this.creation_time = creation_time;
    }
    
    /** Getter for property experation_time.
     * @return Value of property experation_time.
     *
     */
    public long getExperation_time() {
        return experation_time;
    }
    
    /** Setter for property experation_time.
     * @param experation_time New value of property experation_time.
     *
     */
    public void setExperation_time(long experation_time) {
        this.experation_time = experation_time;
    }
    
    /** Getter for property availableLockedSize.
     * @return Value of property availableLockedSize.
     *
     */
    public long getAvailableLockedSize() {
        return availableLockedSize;
    }
    
    /** Setter for property availableLockedSize.
     * @param availableLockedSize New value of property availableLockedSize.
     *
     */
    public void setAvailableLockedSize(long availableLockedSize) {
        this.availableLockedSize = availableLockedSize;
    }
    
}
