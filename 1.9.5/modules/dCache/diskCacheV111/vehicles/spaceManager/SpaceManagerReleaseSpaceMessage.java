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
public class SpaceManagerReleaseSpaceMessage extends SpaceManagerMessage{
    private Long size;
    
    /** Creates a new instance of SpaceManagerReserveSpaceMessage 
     * us this constructor if you want to release all space
     * locked
     */
    public SpaceManagerReleaseSpaceMessage(long spaceToken) {
        super(spaceToken);
    }
 
    
    /** Creates a new instance of SpaceManagerReserveSpaceMessage 
     * use this constractor when you know how much space to release
     */
    public SpaceManagerReleaseSpaceMessage(long spaceToken, long size) {
        super(spaceToken);
        if(size < 0) {
            throw new IllegalArgumentException("size must be nonnegative");
        }
        this.size = new Long (size);
    }
    
    /**
     * Getter for property size.
     * @return Value of property size.
     */
    public Long getSize() {
        return size;
    }
    
    /**
     * Setter for property size.
     * @param size New value of property size.
     */
    public void setSize(Long size) {
        this.size = size;
    }
    
    
}
