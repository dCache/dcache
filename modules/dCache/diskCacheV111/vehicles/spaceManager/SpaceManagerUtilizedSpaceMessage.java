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
public class SpaceManagerUtilizedSpaceMessage extends SpaceManagerMessage{
    private long size;
    
    /** Creates a new instance of SpaceManagerReserveSpaceMessage */
    public SpaceManagerUtilizedSpaceMessage(long spaceToken, long size) {
        super(spaceToken);
        if(size < 0) {
            throw new IllegalArgumentException("size must be nonnegative");
        }
        this.size = size;
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
    
    
}
