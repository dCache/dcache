// $Id$

package diskCacheV111.replicaManager ;

import java.sql.SQLException;
import java.util.Iterator;

import diskCacheV111.util.PnfsId;

public interface ReplicaDb1 extends ReplicaDb {

    String DOWN = "down";
    String ONLINE = "online";
    String OFFLINE = "offline";
    String OFFLINE_PREPARE = "offline-prepare";
    String DRAINOFF = "drainoff";

    Iterator<String> getPnfsIds(String poolName) throws SQLException;
    Iterator<String> getPools() ;
    Iterator<String> getPoolsReadable() ;
    Iterator<String> getPoolsWritable() ;

    Iterator<Object[]> getRedundant(int maxcnt);
    Iterator<Object[]> getDeficient(int mincnt);
    Iterator<String> getMissing();
    Iterator<String> getInDrainoffOnly();
    Iterator<String> getInOfflineOnly();

//     public void addPoolStatus(String poolName, String poolStatus); // removed
void removePoolStatus(String poolName);
    void setPoolStatus(String poolName, String poolStatus);
    String getPoolStatus(String poolName);

    void addTransaction(PnfsId pnfsId, long timestamp, int count);
    void removeTransaction(PnfsId pnfsId);
    long getTimestamp(PnfsId pnfsId);

    void removePool(String poolName) ;

    void setHeartBeat(String name, String desc);
    void removeHeartBeat(String name);

    void clearPool(String poolName); // clear entries in pools and replicas tables
    void clearTransactions();          // clear transactions in action table

}
