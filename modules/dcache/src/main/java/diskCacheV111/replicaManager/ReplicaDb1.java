// $Id$

package diskCacheV111.replicaManager ;

import java.sql.SQLException;
import java.util.Iterator;

import diskCacheV111.util.PnfsId;

public interface ReplicaDb1 extends ReplicaDb {

    static final String DOWN = "down";
    static final String ONLINE = "online";
    static final String OFFLINE = "offline";
    static final String OFFLINE_PREPARE = "offline-prepare";
    static final String DRAINOFF = "drainoff";

    public Iterator<String> getPnfsIds(String poolName) throws SQLException;
    public Iterator<String> getPools( ) ;
    public Iterator<String> getPoolsReadable( ) ;
    public Iterator<String> getPoolsWritable( ) ;

    public Iterator<Object[]> getRedundant(int maxcnt);
    public Iterator<Object[]> getDeficient(int mincnt);
    public Iterator<String> getMissing( );
    public Iterator<String> getInDrainoffOnly( );
    public Iterator<String> getInOfflineOnly( );

//     public void addPoolStatus(String poolName, String poolStatus); // removed
    public void removePoolStatus(String poolName);
    public void setPoolStatus(String poolName, String poolStatus);
    public String getPoolStatus(String poolName);

    public void addTransaction(PnfsId pnfsId, long timestamp, int count);
    public void removeTransaction(PnfsId pnfsId);
    public long getTimestamp(PnfsId pnfsId);

    public void removePool( String poolName ) ;

    public void setHeartBeat(String name, String desc);
    public void removeHeartBeat(String name);

    public void clearPool( String poolName ); // clear entries in pools and replicas tables
    public void clearTransactions();          // clear transactions in action table

}
