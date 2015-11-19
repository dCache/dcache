// $Id$

package diskCacheV111.replicaManager ;

import java.sql.SQLException;
import java.util.Iterator;

import diskCacheV111.util.PnfsId;

public interface ReplicaDb {

    void addPool(PnfsId pnfsId, String poolName) ;
    void removePool(PnfsId pnfsId, String poolName) ;
    int countPools(PnfsId pnfsId) ;
    void clearPools(PnfsId pnfsId) ;
    Iterator<String> getPnfsIds() throws SQLException;
    Iterator<String> getPools(PnfsId pnfsId) ;
    void clearAll() ;
}
