//   $Id: HsmFlushSchedulable.java,v 1.2 2006-04-03 05:51:55 patrick Exp $
package diskCacheV111.hsmControl.flush ;

import java.util.Map;

import org.dcache.util.Args;
/**
 * An implemantation of the HsmFlushScheduler is called by the HsmFlushController
 * in the corresponding events.
 *
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.0, Dec 03, 2005
 *
 */
public interface HsmFlushSchedulable {

     /**
       * Is called after the implementation of this interface has been
       * successfully loaded. Under some circumstances, the 'init' call
       * is delayed until sufficient information could be gathered by
       * the HsmFlushController.
       *
       */
     public void init() ;
     /**
       * Is called if the driver should be prepare for unload. The routine
       * shouldn't return until all resources are returned.
       *
       */
     public void prepareUnload() ;
     /**
       * Is called after a pool reported a flush process to be finished.
       *
       * @param poolName Name of the pool which finished the flush process.
       * @param storageClassName Name of the storage class which finished the flush process.
       * @param flushInfo Detailed information on the storage class of this pool.
       */
     public void flushingDone( String poolName , String storageClassName , HsmFlushControlCore.FlushInfo flushInfo ) ;
     /**
       * Is called whenever new information arrived from a pool.
       *
       * @param poolName Name of the pool which finished the flush process.
       * @param pool Detailed information on the pool.
       */
     public void poolFlushInfoUpdated( String poolName , HsmFlushControlCore.Pool pool ) ;
     /**
       * Is called whenever a 'driver command' was issued from within the HsmFlushController.
       *
       * @param poolName Name of the pool which finished the flush process.
       * @param args Detailed information on the pool.
       */
     public void command( Args args ) ;
     /**
       * Is called whenever the pool setup was updated. This is usually the case if
       * 'configured pools' become active resp. inactive.
       */
     public void poolSetupUpdated() ;
     /**
       * Is called whenever the properties (resp. parameters) of the driver is
       * supposed to change or it may be used to query the current status of
       * properties. The property map will only contain key-value pairs
       * where the value should be updated. On return the driver should put
       * all currenly available property key value pairs into the property map.
       * I should make sure that initial map entries are removed which do not
       * represent a valid property of this driver. Furthermore the returned
       * values should reflect the already modified values of this request.
       * If the arriving property map is empty this call is equivalent to a
       * properties query.
       */
     public void propertiesUpdated( Map<String,Object> properties ) ;
     /**
       * Is frequently called.
       */
     public void timer() ;
     /**
       * Is called the driver should reset its status as if it would have been
       * newly loaded.
       */
     public void reset() ;
     /**
       * Called on adding a pool.
       *
       * @param poolName Name of the pool which was added.
       */
     public void configuredPoolAdded( String poolName ) ;
     /**
       * Called on removing a pool.
       *
       * @param poolName Name of the pool which was removed.
       */
     public void configuredPoolRemoved( String poolName ) ;
     /**
       * Called whenever the I/O mode of a pool might have changed. (From readOnly to readWrite or
       * vise versa). It might happen that the method is called although the
       * mode didn't change.
       *
       * @param poolName Name of the pool which was removed.
       * @param pool Detailed information on the pool.
       */
     public void poolIoModeUpdated( String poolName ,  HsmFlushControlCore.Pool pool ) ;

}
