// $Id: ReplicaDb.java,v 1.3.24.1 2007-10-12 23:35:36 aik Exp $

package diskCacheV111.replicaManager ;

import  diskCacheV111.util.* ;

import  java.util.* ;

public interface ReplicaDb {
    
    public void addPool( PnfsId pnfsId , String poolName ) ;
    public void removePool( PnfsId pnfsId , String poolName ) ;
    public int countPools( PnfsId pnfsId ) ;
    public void clearPools( PnfsId pnfsId ) ;
    public Iterator pnfsIds() ;
    public Iterator getPools( PnfsId pnfsId ) ;
    public void clearAll() ;
}
