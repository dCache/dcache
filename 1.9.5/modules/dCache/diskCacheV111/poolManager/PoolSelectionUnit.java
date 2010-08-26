package diskCacheV111.poolManager ;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.StorageInfo;
import java.net.UnknownHostException;

public interface PoolSelectionUnit  {

    /**
     * data flow direction. Supported modes are:
     * <pre>
     *      READ - read cached file from a pool
     *      WRITE - write a new file into a pool
     *      CACHE - get a file from a tape
     *      P2P   - internal pool 2 pool transfer
     *      ANY   - any of above
     * </pre>
     */
    public enum DirectionType {
        READ,
        WRITE,
        CACHE,
        P2P,
        ANY,
    }

   public interface SelectionLink {
      public String getName() ;
      public Iterator<SelectionPool> pools() ;
      public String  getTag() ;
   }

    /**
     * Encapsulates information about a pool. The information is
     * updated periodically. Due to the distributed nature of dCache,
     * the information may be outdated.
     */
    public interface SelectionPool
    {
        public String  getName();

        /**
         * Returns the time in milliseconds since the last heartbeat
         * from the pool.
         */
        public long    getActive();

        /**
         * Sets whether the pool is active or not. This is also used to
         * deliver a hearbeat, i.e. calling this method with an
         * argument of 'true' will reset the heartbeat counter.
         */
        public void    setActive(boolean active);

        /**
         * Returns true if the pool has been marked as read-only in the
         * pool manager. Notice that this is not the same as whether
         * the pool can actually write, as there are other places that
         * a pool can be marked read-only.
         */
        public boolean isReadOnly();
        /**
         * Sets the read-only flag.
         */
        public void    setReadOnly(boolean rdOnly);

        /**
         * Returns true if the pool is enabled. This is the case if no
         * operations of the pool have been disabled.
         */
        public boolean isEnabled() ;

        /**
         * Sets the ID of the pool. Used to detect when a pool was
         * restarted. Returns true if and only if the serial ID was changed.
         */
        public boolean setSerialId(long serialId);

        /**
         * Returns true if the pool is marked as active. This is
         * normally the case if a heartbeat has been received within
         * the last five minutes. Notice that this is unrelated to
         * isEnabled(), e.g., a disabled pool can be active.
         */
        public boolean isActive();

        /**
         * Sets the pool mode. The pool mode defines which operations
         * are enabled at the pool.
         */
        public void setPoolMode(PoolV2Mode mode);

        /**
         * Returns the pool mode.
         *
         * @see setPoolMode
         */
        public PoolV2Mode getPoolMode();

        /**
         * Returns whether the pool can perform read operations.
         */
        public boolean canRead();

        /**
         * Returns whether the pool can perform write operations.
         */
        public boolean canWrite();

        /**
         * Returns whether the pool can perform stage files from tape
         * operations.
         */
        public boolean canReadFromTape();

        /**
         * Returns whether the pool can perform serve files for P2P
         * operations.
         */
        public boolean canReadForP2P();

        /** Returns the names of attached HSM instances. */
        public Set<String> getHsmInstances();

        /** Sets the set of names of attached HSM instances. */
        public void setHsmInstances(Set<String> hsmInstances);
    }

   public interface SelectionLinkGroup {
	   public String  getName() ;
	   public void add(SelectionLink link);
	   public boolean remove(SelectionLink link);
	   Collection<SelectionLink> links();
	   void attribute(String attribute, String value, boolean replace);
	   Set<String> attribute(String attribute);
	   void removeAttribute(String attribute, String value);
	   Map<String, Set<String>> attributes();

       void setCustodialAllowed(boolean isAllowed);
       void setOutputAllowed(boolean isAllowed);
       void setReplicaAllowed(boolean isAllowed);
       void setOnlineAllowed(boolean isAllowed);
       void setNearlineAllowed(boolean isAllowed);

	   public boolean isCustodialAllowed();
	   public boolean isOutputAllowed();
	   public boolean isReplicaAllowed();
	   public boolean isOnlineAllowed();
	   public boolean isNearlineAllowed();
	}
   public SelectionPool getPool( String poolName ) ;
   public SelectionPool getPool( String poolName , boolean create ) ;
   public SelectionLink getLinkByName( String linkName ) throws NoSuchElementException ;
   public PoolPreferenceLevel []
            match( DirectionType type ,
                   String store , String dcache , String net , String protocol,
                   StorageInfo info, String linkGroup ) ;
   public String [] getActivePools() ;
   public String [] getDefinedPools( boolean enabledOnly ) ;
   public String    getVersion() ;
   public String getNetIdentifier( String address ) throws UnknownHostException;
   public String getProtocolUnit( String protocolUnitName ) ;
   public SelectionLinkGroup getLinkGroupByName(String linkGroupName) throws NoSuchElementException ;
   public String [] getLinkGroups();
   public String [] getLinksByGroupName(String linkGroupName) throws NoSuchElementException ;
   public Collection<SelectionPool> getPoolsByPoolGroup(String poolGroup) throws NoSuchElementException;
}
