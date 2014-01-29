//   $Id: HsmFlushControlCore.java,v 1.3 2006-07-27 22:00:17 patrick Exp $
package diskCacheV111.hsmControl.flush ;

import java.util.List;
import java.util.Set;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.StorageClassFlushInfo;

import org.dcache.util.Args;
/**
 * Whenever a HsmFlushControlManager cell loads a driver it provides
 * an implementation of this class to enable a drive to query resp.
 * set the status of pools, it is responible for.
 *
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.9, Dec 03, 2005
 *
 */
public interface HsmFlushControlCore {

    /**
      * Implementations of the Pool interface allows the driver to query
      * detailed information of the associated pool.
      */
    public interface PoolDetails {
       /**
         *  Returns the name of this pool.
         *
         * @return Name of this pool.
         *
         */
       public String getName() ;
       /**
         *  Provides the PoolCellInfo of this pool.
         *
         * @return PoolCellInfo of the pool.
         *
         */
       public PoolCellInfo getCellInfo() ;
       /**
         *  Returns whether or not this pool is flushing one or more storage classes.
         *
         * @return True if pool is in the process of flushing one or more storage classes.
         *
         */
       public boolean isActive()  ;
       /**
         *  Returns a List of FlushInfos, containing files to be flushed,
         *  or in process of being flushed.
         *
         * @return List of flush infos on this pool.
         *
         */
       public List<FlushInfoDetails> getFlushInfos() ;
       /**
         * Returns the I/O mode of this pool. If 'true' the pool has been set 'rdonly'.
         * otherwise read and writes are allowed.
         */
       public boolean isReadOnly() ;

    }
    /**
      * Implementations of the Pool interface allows the driver to query
      * detailed information of the associated pool. In addition, it allows
      * to store and retrieve a user pointer for convenience.
      */
    public interface Pool extends PoolDetails {
       /**
         *  Provides the internal flush representation of the specified storage class.
         *
         * @param storageClass Name of the storage class. The syntax of a
         *        storage class may differ between dCaches with different
         *        HSM systems attached.
         * @return Internal  representation of the flush information
         *         for the storage class. Returns the null pointer if the
         *         provided storage class doesn't exist.
         *
         */
       public FlushInfo getFlushInfoByStorageClass( String storageClass ) ;
       /**
         *  Returns a set of storage classes (string), containing files to be flushed,
         *  or in the process of being flushed.
         *
         * @return Set of storage class names (strings).
         *
         */
       public Set<String> getStorageClassNames() ;
       /**
         *  The queryMode method initiats a i/o mode query for this pool.
         *  The result is returned by the poolIoModeUpdated of the HsmFlushSchedulable class.
         *
         *
         */
       public void queryMode() ;
       /**
         *  Sends the request to change the I/O mode to the specified one.
         *
         * @param rdOnly If true the pool should be set to rdOnly , otherwise the pool should
         *        be set to read/write.
         *
         */
       public void setReadOnly( boolean rdOnly ) ;
       /**
         *  Stores an arbitray object provided by the driver implementation.
         *  The DriverHandle may be any class implementing the empty DriverHandle interface.
         *
         * @return Set of storage class names (strings).
         *
         */
       public void setDriverHandle( DriverHandle handle ) ;
       /**
         *  Returns the driver handle which had been previously stored by setDriverHandle.
         *  Returns the null pointer if no DriverHandle has been stored.
         *  The DriverHandle may be any class implementing the empty DriverHandle interface.
         *
         * @param Any object implementing the (empty) DriverHandle interface.
         *
         */
       public DriverHandle getDriverHandle() ;
       /**
         * Returns whether or not the I/O mode of this pool is known. If 'true'
         * isReadOnly returns the correct value, otherwise 'isReadOnly' returns
         * an arbitrary value.
         */
       public boolean isPoolIoModeKnown() ;
    }
    /**
      * Implementations of the FlushInfoDetails interface allow the flushDriver to
      * query the flush behaviour of individual StorageClasses
      * of the assigned pool.
      */
    public interface FlushInfoDetails {
       /**
         * Get the name of the corresponding storage class.
         *
         *  @return Name of the corresponding storage class.
         */
       public String getName() ;
       /**
         *  Returns true if the StorageClass is currently flushing its precious files
         *  otherwise 'false'.
         *
         * @return Determines whether or not this storage class is currently flushing it's contents.
         */
       public boolean isFlushing() ;
       /**
         *  Provides the StorageClassFlushInfo of this storage class;
         *
         *  @return The StorageClassFlushInfo of this storage class;
         *
         */
       public StorageClassFlushInfo getStorageClassFlushInfo();

    }
    /**
      * Implementations of the FlushInfo interface allow the flushDriver to
      * query and influence the flush behaviour of individual StorageClasses
      * of the assigned pool.
      */
    public interface FlushInfo extends FlushInfoDetails {
       /**
         * Initiate a flush of 'n' or all files of a storage class within a pool.
         *
         *  @param count Number of files to flush out of the set of files waiting in this
         *            storage class. If count is zero, all files are flushed.
         *           The result of the flush operation can be monitored by
         *           the various callbacks of the HsmFlushSchedulable interface.
         */
       public void flush( int count ) throws Exception ;

    }
    /**
      *  Classes which are intended to be stored and retrieved by the corresponding
      *  set/getDriverHandle methods of the Pool class needs to implement this interface.
      *  This interface doesn't contain any methods.
      *
      */
    public interface DriverHandle {

    }

    /**
      *  Returns the options given while this driver was created.
      *  If this driver was loaded with the -scheduler option
      *  on the cell command line, Args contains all other options
      *  of the command line. If the driver was loaded, using the
      *  'load' command of the corresponding HsmFlushControlManager,
      *  'Args' contain the options of the 'load' command.
      *
      *  @return Options from the command line of the driver start.
      */
    public Args getDriverArgs() ;
    /**
      *  Provides the internal pool object of the given name.
      * @param poolName Name of the pool for which the internal
      *          pool object should be returned.
      * @return Internal pool object of the name given. Returns the null
      *         pointer if no pool exists with the given name.
      */
    public Pool getPoolByName( String poolName ) ;
    /**
      *  Provides the set of pool, this driver is responsible for.
      *
      * @return A set of pool names (String) which should be managed by this driver.
      */
    public Set<String> getConfiguredPoolNames() ;
    /**
      *  Provides a list of pool objects, this driver is responsible for.
      *
      * @return A list of 'Pool' objects which  should be managed by this driver.
      */
    public List<Pool> getConfiguredPools() ;
}
