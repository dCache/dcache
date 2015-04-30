package diskCacheV111.hsmControl.flush;

import java.io.Serializable;

import diskCacheV111.pools.StorageClassFlushInfo;

/**
 * Whenever a HsmFlushControlManager cell loads a driver it provides
 * an implementation of this class to enable a drive to query resp.
 * set the status of pools, it is responible for.
 *
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.9, Dec 03, 2005
 *
 */
public class HsmFlushControllerFlushInfoDetails implements HsmFlushControlCore.FlushInfoDetails, Serializable {

     private static final long serialVersionUID = -8771095162272613672L;
     /* package */ String  _name;
     /* package */ StorageClassFlushInfo _flushInfo;
     /* package */ boolean _isFlushing;

     /**
       * Get the name of the corresponding storage class.
       *
       *  @return Name of the corresponding storage class.
       */
     @Override
     public String getName(){ return _name ; }
     /**
       *  Returns true if the StorageClass is currently flushing its precious files
       *  otherwise 'false'.
       *
       * @return Determines whether or not this storage class is currently flushing it's contents.
       */
     @Override
     public boolean isFlushing(){ return _isFlushing ; }
     /**
       *  Provides the StorageClassFlushInfo of this storage class;
       *
       *  @return The StorageClassFlushInfo of this storage class;
       *
       */
     @Override
     public StorageClassFlushInfo getStorageClassFlushInfo(){ return _flushInfo ; }


}
