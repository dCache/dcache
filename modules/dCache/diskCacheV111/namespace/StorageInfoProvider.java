/*
 * $Id: StorageInfoProvider.java,v 1.1 2005-08-11 08:35:28 tigran Exp $
 */
package diskCacheV111.namespace;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

/**
 * @author tigran
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public interface StorageInfoProvider extends DcacheNameSpaceProvider {
    StorageInfo getStorageInfo( PnfsId pnfsId) throws Exception;
    void setStorageInfo( PnfsId pnfsId, StorageInfo storageInfo, int mode) throws Exception;

}
