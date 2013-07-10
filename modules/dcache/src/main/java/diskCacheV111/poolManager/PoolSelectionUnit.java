package diskCacheV111.poolManager ;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellAddressCore;

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
    public interface SelectionEntity {

        public String getName();
    }
   public interface SelectionLink extends SelectionEntity{
      /**
       * Get a defensive copy of the pools defined accessible through this link.
       * @return collection of pools
       */
      public Collection<SelectionPool> getPools() ;
      public String  getTag() ;
      public LinkReadWritePreferences getPreferences();
      public Collection<SelectionPoolGroup> getPoolGroupsPointingTo();
      public Collection<SelectionUnitGroup> getUnitGroupsTargetedBy();
   }

    /**
     * Encapsulates information about a pool. The information is
     * updated periodically. Due to the distributed nature of dCache,
     * the information may be outdated.
     */
    public interface SelectionPool extends SelectionEntity
    {

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

        public Collection<SelectionPoolGroup> getPoolGroupsMemberOf();
        public Collection<SelectionLink> getLinksTargetingPool();

        CellAddressCore getAddress();

        void setAddress(CellAddressCore address);
    }
    public interface SelectionPoolGroup extends SelectionEntity {
    }
   public interface SelectionLinkGroup extends SelectionEntity{
	   public void add(SelectionLink link);
	   public boolean remove(SelectionLink link);
	   Collection<SelectionLink> getLinks();
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
    public interface SelectionUnit extends SelectionEntity{
        public String getUnitType();
        public Collection<SelectionUnitGroup> getMemberOfUnitGroups();
    }

    public interface SelectionUnitGroup extends SelectionEntity {
        public Collection<SelectionUnit> getMemeberUnits();
        public Collection<SelectionLink> getLinksPointingTo();
    }
   public SelectionPool getPool( String poolName ) ;
   public SelectionPool getPool( String poolName , boolean create ) ;
   public SelectionLink getLinkByName( String linkName ) throws NoSuchElementException ;
   public PoolPreferenceLevel []
            match( DirectionType type, String net , String protocol,
                   StorageInfo info, String linkGroup ) ;
   public String [] getActivePools() ;
   public String [] getDefinedPools( boolean enabledOnly ) ;
   public String    getVersion() ;
   public String getNetIdentifier( String address ) throws UnknownHostException;
   public String getProtocolUnit( String protocolUnitName ) ;
   public SelectionLinkGroup getLinkGroupByName(String linkGroupName) throws NoSuchElementException ;
   public Collection<SelectionPool> getPoolsByPoolGroup(String poolGroup) throws NoSuchElementException;
   public Collection<SelectionPool> getAllDefinedPools( boolean enabledOnly ) ;
   public Collection<SelectionPoolGroup> getPoolGroupsOfPool(String PoolName);
   public Collection<SelectionLink> getLinksPointingToPoolGroup(String poolGroup) throws NoSuchElementException;

   public Map<String, SelectionLink> getLinks();
   public Map<String, SelectionPool> getPools();
   public Map<String, SelectionPoolGroup> getPoolGroups();
   public Map<String, SelectionLinkGroup> getLinkGroups();
   public Map<String, SelectionUnit> getSelectionUnits();
   public Map<String, SelectionUnitGroup> getUnitGroups();
}
