/*
 * $Id: StorageInfoProvider.java,v 1.2 2007-07-10 17:15:09 tigran Exp $
 */
package diskCacheV111.namespace;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

public interface StorageInfoProvider extends DcacheNameSpaceProvider {
	
	/**
	 * set if there is no old value
	 */
	public static final int SI_EXCLUSIVE = 0;
	/**
	 * replace old value with new one
	 */
	public static final int SI_OVERWRITE = 1;
	/**
	 * append new value to the old one
	 */
	public static final int SI_APPEND = 2;
	
    StorageInfo getStorageInfo( PnfsId pnfsId) throws Exception;
    void setStorageInfo( PnfsId pnfsId, StorageInfo storageInfo, int mode) throws Exception;

}
