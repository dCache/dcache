/*
 * $Id: CacheLocationProvider.java,v 1.1 2005-08-11 08:35:28 tigran Exp $
 */
package diskCacheV111.namespace;

import java.util.List;

import diskCacheV111.util.PnfsId;

/**
 * @author tigran
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public interface CacheLocationProvider extends DcacheNameSpaceProvider {
    void addCacheLocation(PnfsId pnfsId, String cacheLocation) throws Exception ;
    List getCacheLocation(PnfsId pnfsId) throws Exception ;
    void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws Exception ;
}
