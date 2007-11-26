/*
 * $Id: CacheLocationProvider.java,v 1.3 2007-09-20 14:25:42 tigran Exp $
 */
package diskCacheV111.namespace;

import java.util.List;

import diskCacheV111.util.PnfsId;

/**
 * cache locations handling interface
 */
public interface CacheLocationProvider extends DcacheNameSpaceProvider {
	
	/**
	 * add a cache location for a file
	 * @param pnfsId of the file
	 * @param cacheLocation the new location
	 * @throws Exception
	 */
    void addCacheLocation(PnfsId pnfsId, String cacheLocation) throws Exception ;
    
    /**
     * get all cache location of the file
     * @param pnfsId of the file
     * @return list containing locations or empty list, if locations are unknown
     * @throws Exception
     */
    List<String> getCacheLocation(PnfsId pnfsId) throws Exception ;
    
    /**
     * clear cache locations 
     * @param pnfsId of the file
     * @param cacheLocation, "*" forces to remove all known locations
     * @param removeIfLast remove entry from namespace if last known location is removed
     * @throws Exception
     */
    void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws Exception ;
}
