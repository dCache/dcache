package org.dcache.webadmin.model.dataaccess;

import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.pools.PoolV2Mode;

import org.dcache.poolmanager.Partition;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * A PoolsDAO implements the services to get the Data out of a dCache that is
 * needed to display information about Pools.
 * @author jan schaefer 29-10-2009
 */
public interface PoolsDAO {

    /**
     * @param poolGroup poolgroup asked for containing pools
     * @return delivers a list of Pools in given pool group
     */
    public Set<Pool> getPoolsOfPoolGroup(String poolGroup) throws DAOException;

    /**
     *
     * @return delivers a list of Pools in dCache
     */
    public Set<Pool> getPools() throws DAOException;

    /**
     *
     * @return delivers a list of names of all Poolgroups in dCache
     */
    public Set<String> getPoolGroupNames() throws DAOException;

    /**
     *
     * @return delivers a list of Poolgroups in dCache
     */
    public Set<SelectionPoolGroup> getPoolGroups() throws DAOException;

    /**
     * @param poolName asking for poolgroups this pool is member of
     * @return delivers a list of Poolgroups the asked pool is member of
     */
    public Set<SelectionPoolGroup> getPoolGroupsOfPool(String poolName) throws DAOException;

    /**
     *
     * @return delivers a list of all Links in dCache
     */
    public Set<SelectionLink> getLinks() throws DAOException;

    /**
     *
     * @return delivers a list of all Units in dCache
     */
    public Set<SelectionUnit> getUnits() throws DAOException;

    /**
     *
     * @return delivers a list of all Unit Groups in dCache
     */
    public Set<SelectionUnitGroup> getUnitGroups() throws DAOException;

    /**
     * @param poolGroup asking for links pointing to this poolgroup
     * @return delivers a list of Links pointing to given poolgroupname
     */
    public Set<SelectionLink> getLinksPointingToPoolGroup(String poolGroup) throws DAOException;

    /**
     * @return delivers a list of pool preferences that match the supplied values
     */
    public PoolPreferenceLevel[] match(DirectionType type, String netUnitName,
            String protocolUnitName, String hsm, String storageClass,
            String linkGroupName) throws DAOException;

    /**
     *
     * @param poolIds pools to change mode
     * @param poolMode mode to change to
     * @param userName user who calls the method
     */
    public void changePoolMode(Set<String> poolIds, PoolV2Mode poolMode, String userName)
            throws DAOException;

    /**
     * @return returns a Map with name of the Partition as key and the Partition
     * itself as a value
     */
    public Map<String, Partition> getPartitions() throws DAOException;
}
