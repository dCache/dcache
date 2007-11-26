package diskCacheV111.hsmControl.flush ;
import diskCacheV111.pools.* ;
import java.util.* ;

/**
 * Whenever a HsmFlushControlManager cell loads a driver it provides
 * an implementation of this class to enable a drive to query resp.
 * set the status of pools, it is responible for.
 *
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.9, Dec 03, 2005
 *
 */
public class HsmFlushControllerPoolDetails implements HsmFlushControlCore.PoolDetails, java.io.Serializable {

     /* package */ String       _name       = null ;
     /* package */ PoolCellInfo _cellInfo   = null ;
     /* package */ boolean      _isActive   = false ;
     /* package */ boolean      _isReadOnly = false ;
     /* package */ List         _flushInfos = null ;
     /**
       *  Returns the name of this pool.
       *
       * @return Name of this pool.
       *  
       */
     public String getName(){ return _name ; }
     /**
       *  Provides the PoolCellInfo of this pool.
       *
       * @return PoolCellInfo of the pool.
       *  
       */
     public PoolCellInfo getCellInfo(){ return _cellInfo ; }
     /**
       *  Returns whether or not this pool is flushing one or more storage classes.
       *
       * @return True if pool is in the process of flushing one or more storage classes.
       *  
       */
     public boolean isActive() { return _isActive ; }
     /**
       *  Returns a List of FlushInfos, containing files to be flushed,
       *  or in process of being flushed.
       *
       * @return List of flush infos on this pool.
       *  
       */
     public List getFlushInfos(){ return _flushInfos ; }
     /**
       * Returns the I/O mode of this pool. If 'true' the pool has been set 'rdonly'.
       * otherwise read and writes are allowed.
       */
     public boolean isReadOnly(){ return _isReadOnly ; }

}
