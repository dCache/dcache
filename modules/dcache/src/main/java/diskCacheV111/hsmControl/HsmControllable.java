/*
 * HsmControllable.java
 *
 * Created on January 16, 2005, 6:58 PM
 */

package diskCacheV111.hsmControl;

import diskCacheV111.vehicles.StorageInfo;
/**
 *
 * @author  patrick
 */
public interface HsmControllable {

    void getBfDetails(StorageInfo storageInfo) throws Exception ;

}
