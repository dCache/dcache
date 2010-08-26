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
public class SpaceManagerReserveSpaceMessage  extends SpaceManagerMessage{
    //request parameters
    int uid;
    int gid;
    private String path;
    private long  size; //if specified, this is the
    private String hostToTransferIntoSpace;
    private long lifetime;
    private long creation_time;
    private long expiration_time;
    private StorageInfo storageInfo;
    private PnfsId pnfsId;
    
    /** Creates a new instance of SpaceManagerReserveSpaceMessage */
    public SpaceManagerReserveSpaceMessage(int uid, int gid,String path, long size,String hostToTransferIntoSpace, long lifetime) {
        if(path == null || hostToTransferIntoSpace == null) {
            throw new NullPointerException("path and  host must not be null");
        }
        this.uid = uid;
        this.gid = gid;
        this.path = path;
        this.size = size;
        this.hostToTransferIntoSpace = hostToTransferIntoSpace;
        this.lifetime = lifetime;
    }
 
       /** Creates a new instance of SpaceManagerReserveSpaceMessage */
    public SpaceManagerReserveSpaceMessage(String path, long size,StorageInfo storageInfo,PnfsId pnfsId, String hostToTransferIntoSpace, long lifetime) {
        if(path == null || storageInfo == null || pnfsId == null) {
            throw new NullPointerException("path and  storage info must not be null");
        }
        this.path = path;
        this.size = size;
        this.storageInfo = storageInfo;
        this.pnfsId = pnfsId;
        this.hostToTransferIntoSpace = hostToTransferIntoSpace;
        this.lifetime = lifetime;
    }

    public SpaceManagerReserveSpaceMessage(long id) {
        super(id);

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
     * Getter for property size.
     * @return Value of property size.
     */
    public long getSize() {
        return size;
    }
    
    /**
     * Setter for property size.
     * @param size New value of property size.
     */
    public void setSize(long size) {
        this.size = size;
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
    
    
    /**
     * Getter for property lifetime.
     * @return Value of property lifetime.
     */
    public long getLifetime() {
        return lifetime;
    }
    
    /**
     * Setter for property lifetime.
     * @param lifetime New value of property lifetime.
     */
    public void setLifetime(long lifetime) {
        this.lifetime = lifetime;
    }
    
    /**
     * Getter for property storageInfo.
     * @return Value of property storageInfo.
     */
    public diskCacheV111.vehicles.StorageInfo getStorageInfo() {
        return storageInfo;
    }
    
    /**
     * Setter for property storageInfo.
     * @param storageInfo New value of property storageInfo.
     */
    public void setStorageInfo(diskCacheV111.vehicles.StorageInfo storageInfo) {
        this.storageInfo = storageInfo;
    }
    
    /**
     * Getter for property pnfsId.
     * @return Value of property pnfsId.
     */
    public diskCacheV111.util.PnfsId getPnfsId() {
        return pnfsId;
    }
    
    /**
     * Setter for property pnfsId.
     * @param pnfsId New value of property pnfsId.
     */
    public void setPnfsId(diskCacheV111.util.PnfsId pnfsId) {
        this.pnfsId = pnfsId;
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
    
    /** Getter for property expiration_time.
     * @return Value of property expiration_time.
     *
     */
    public long getExpiration_time() {
        return expiration_time;
    }
    
    /** Setter for property expiration_time.
     * @param expiration_time New value of property expiration_time.
     *
     */
    public void setExpiration_time(long expiration_time) {
        this.expiration_time = expiration_time;
    }
    
    /**
     * Getter for property uid.
     * @return Value of property uid.
     */
    public int getUid() {
        return uid;
    }
    
    /**
     * Setter for property uid.
     * @param uid New value of property uid.
     */
    public void setUid(int uid) {
        this.uid = uid;
    }
    
    /**
     * Getter for property gid.
     * @return Value of property gid.
     */
    public int getGid() {
        return gid;
    }
    
    /**
     * Setter for property gid.
     * @param gid New value of property gid.
     */
    public void setGid(int gid) {
        this.gid = gid;
    }
    
}
