package diskCacheV111.poolManager;

import diskCacheV111.pools.PoolV2Mode;
import dmg.cells.nucleus.CellAddressCore;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.dcache.vehicles.FileAttributes;

public interface PoolSelectionUnit {

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
    enum DirectionType {
        READ,
        WRITE,
        CACHE,
        P2P,
        ANY,
    }

    /**
     * Selection unit types.
     */
    enum UnitType {
        STORE,
        DCACHE,
        NET,
        PROTOCOL
    }

    interface SelectionEntity {

        String getName();
    }

    interface SelectionLink extends SelectionEntity {

        /**
         * Get a defensive copy of the pools defined accessible through this link.
         *
         * @return collection of pools
         */
        Collection<SelectionPool> getPools();

        String getTag();

        LinkReadWritePreferences getPreferences();

        Collection<SelectionPoolGroup> getPoolGroupsPointingTo();

        Collection<SelectionUnitGroup> getUnitGroupsTargetedBy();
    }

    /**
     * Encapsulates information about a pool. The information is updated periodically. Due to the
     * distributed nature of dCache, the information may be outdated.
     */
    interface SelectionPool extends SelectionEntity {

        /**
         * Returns the time in milliseconds since the last heartbeat from the pool.
         */
        long getActive();

        /**
         * Sets whether the pool is active or not. This is also used to deliver a hearbeat, i.e.
         * calling this method with an argument of 'true' will reset the heartbeat counter.
         */
        void setActive(boolean active);

        /**
         * Returns true if the pool has been marked as read-only in the pool manager. Notice that
         * this is not the same as whether the pool can actually write, as there are other places
         * that a pool can be marked read-only.
         */
        boolean isReadOnly();

        /**
         * Sets the read-only flag.
         */
        void setReadOnly(boolean rdOnly);

        /**
         * Returns true if the pools hsmInstances are equal  to the given ones
         */
        boolean hasAnyHsmFrom(Collection<String> hsm);

        /**
         * Returns true if the pool is enabled. This is the case if no operations of the pool have
         * been disabled.
         */
        boolean isEnabled();

        /**
         * Sets the ID of the pool. Used to detect when a pool was restarted. Returns true if and
         * only if the serial ID was changed.
         */
        boolean setSerialId(long serialId);

        long getSerialId();

        /**
         * Returns true if the pool is marked as active. This is normally the case if a heartbeat
         * has been received within the last five minutes. Notice that this is unrelated to
         * isEnabled(), e.g., a disabled pool can be active.
         */
        boolean isActive();

        /**
         * Sets the pool mode. The pool mode defines which operations are enabled at the pool.
         */
        void setPoolMode(PoolV2Mode mode);

        /**
         * Returns the pool mode.
         *
         * @see #setPoolMode
         */
        PoolV2Mode getPoolMode();

        /**
         * Returns whether the pool can perform read operations.
         */
        boolean canRead();

        /**
         * Returns whether the pool can perform write operations.
         */
        boolean canWrite();

        /**
         * Returns whether the pool can perform stage files from tape operations.
         */
        boolean canReadFromTape();

        /**
         * Returns whether the pool can perform serve files for P2P operations.
         */
        boolean canReadForP2P();

        /**
         * Returns the names of attached HSM instances.
         */
        Set<String> getHsmInstances();

        /**
         * Sets the set of names of attached HSM instances.
         */
        void setHsmInstances(Set<String> hsmInstances);

        /**
         * Get key-value map with tags associated with the pool.
         *
         * @return tags associated with the pool.
         */
        Map<String, String> getTags();

        /**
         * Set key-value map of tags associated with the pool.
         *
         * @param tags map of tags associated with the pool.
         */
        void setTags(Map<String, String> tags);

        Collection<SelectionPoolGroup> getPoolGroupsMemberOf();

        Collection<SelectionLink> getLinksTargetingPool();

        CellAddressCore getAddress();

        void setAddress(CellAddressCore address);

        void setCanonicalHostName(String hostName);

        Optional<String> getCanonicalHostName();
    }

    interface SelectionPoolGroup extends SelectionEntity {

        boolean isPrimary();

        boolean isResilient();
    }

    interface SelectionLinkGroup extends SelectionEntity {

        void add(SelectionLink link);

        boolean remove(SelectionLink link);

        Collection<SelectionLink> getLinks();

        void setCustodialAllowed(boolean isAllowed);

        void setOutputAllowed(boolean isAllowed);

        void setReplicaAllowed(boolean isAllowed);

        void setOnlineAllowed(boolean isAllowed);

        void setNearlineAllowed(boolean isAllowed);

        boolean isCustodialAllowed();

        boolean isOutputAllowed();

        boolean isReplicaAllowed();

        boolean isOnlineAllowed();

        boolean isNearlineAllowed();
    }

    interface SelectionUnit extends SelectionEntity {

        UnitType getType();

        Collection<SelectionUnitGroup> getMemberOfUnitGroups();
    }

    interface SelectionUnitGroup extends SelectionEntity {

        Collection<SelectionUnit> getMemberUnits();

        Collection<SelectionLink> getLinksPointingTo();
    }

    SelectionPool getPool(String poolName);

    /**
     * Update pool status and information. If pool is already known to PoolSelectionUnitalready pool
     * status is not changed, then no update will happen.
     *
     * @param poolName     name of the pool.
     * @param address      pools cells address.
     * @param serialId     pools restart indication.
     * @param mode         of in which pool pools operate.
     * @param hsmInstances HSM instances to which pool is connected.
     * @param tags         tags attached to pool.
     * @return true iff pool status is changed.
     */
    boolean updatePool(String poolName, CellAddressCore address, String canonicalHostName,
          long serialId,
          PoolV2Mode mode, Set<String> hsmInstances, Map<String, String> tags);

    SelectionLink getLinkByName(String linkName) throws NoSuchElementException;

    PoolPreferenceLevel[]
    match(DirectionType type, String net, String protocol,
          FileAttributes fileAttributes, String linkGroup, Predicate<String> exclude);

    String[] getActivePools();

    String[] getDefinedPools(boolean enabledOnly);

    String getVersion();

    String getNetIdentifier(String address) throws UnknownHostException;

    String getProtocolUnit(String protocolUnitName);

    StorageUnit getStorageUnit(String storageClass);

    SelectionLinkGroup getLinkGroupByName(String linkGroupName) throws NoSuchElementException;

    Collection<SelectionPool> getPoolsByPoolGroup(String poolGroup) throws NoSuchElementException;

    Collection<SelectionPool> getAllDefinedPools(boolean enabledOnly);

    Collection<SelectionPoolGroup> getPoolGroupsOfPool(String PoolName);

    Collection<SelectionLink> getLinksPointingToPoolGroup(String poolGroup)
          throws NoSuchElementException;

    Map<String, SelectionLink> getLinks();

    Map<String, SelectionPool> getPools();

    Map<String, SelectionPoolGroup> getPoolGroups();

    Map<String, SelectionLinkGroup> getLinkGroups();

    Map<String, SelectionUnit> getSelectionUnits();

    Map<String, SelectionUnitGroup> getUnitGroups();

    boolean isEnabledRegex();
}
