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
public class HsmFlushControllerFlushInfoDetails implements HsmFlushControlCore.FlushInfoDetails, java.io.Serializable {

     /* package */ String  _name                    = null ;
     /* package */ StorageClassFlushInfo _flushInfo = null ;
     /* package */ boolean _isFlushing              = false ;

     /**
       * Get the name of the corresponding storage class.
       *
       *  @return Name of the corresponding storage class.
       */
     public String getName(){ return _name ; }
     /**
       *  Returns true if the StorageClass is currently flushing its precious files
       *  otherwise 'false'.
       *
       * @return Determines whether or not this storage class is currently flushing it's contents.
       */
     public boolean isFlushing(){ return _isFlushing ; }
     /**
       *  Provides the StorageClassFlushInfo of this storage class;
       *
       *  @return The StorageClassFlushInfo of this storage class;
       *
       */
     public StorageClassFlushInfo getStorageClassFlushInfo(){ return _flushInfo ; }


}
