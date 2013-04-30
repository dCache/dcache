// $Id$

package diskCacheV111.replicaManager ;

import java.sql.SQLException;
import java.util.Iterator;

import diskCacheV111.util.PnfsId;

public interface ReplicaDb {

    public void addPool( PnfsId pnfsId , String poolName ) ;
    public void removePool( PnfsId pnfsId , String poolName ) ;
    public int countPools( PnfsId pnfsId ) ;
    public void clearPools( PnfsId pnfsId ) ;
    public Iterator<String> getPnfsIds() throws SQLException;
    public Iterator<String> getPools( PnfsId pnfsId ) ;
    public void clearAll() ;
}
