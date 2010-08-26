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
public class SpaceManagerGetInfoAndLockReservationByPathMessage extends SpaceManagerGetInfoAndLockReservationMessage{
      
    /** Creates a new instance of SpaceManagerGetReservationInfoByPathMessage */
    public SpaceManagerGetInfoAndLockReservationByPathMessage(String path) {
        
        if(path == null) {
            throw new NullPointerException("path and  host must not be null");
        }
        this.path = path;
    }
 
}
