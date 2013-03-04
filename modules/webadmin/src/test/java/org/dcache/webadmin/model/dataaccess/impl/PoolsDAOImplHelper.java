package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Collections;
import java.util.HashSet;
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
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * This class is the PoolsDAO for most unit tests, so that there is no need to
 * have a running dcache to run the Unittests. It is possible to add customized
 * pools.
 * @author jans
 */
public class PoolsDAOImplHelper implements PoolsDAO {

    private Set<String> _poolGroups = new HashSet<>();
    private boolean _alwaysThrowsDaoException = false;
    private Set<Pool> _pools = new HashSet();

    public PoolsDAOImplHelper() {
        _pools = XMLDataGathererHelper.getExpectedPools();
        _poolGroups.add(XMLDataGathererHelper.POOL1_POOLGROUP1);
        _poolGroups.add("testgroup1");
    }

    @Override
    public Set<String> getPoolGroupNames() throws DAOException {
        if (_alwaysThrowsDaoException) {
            throw new DAOException("you are a bad boy!");
        }
        return _poolGroups;
    }

    public void setAlwaysThrowsDaoException(boolean alwaysThrowsDaoException) {
        _alwaysThrowsDaoException = alwaysThrowsDaoException;
    }

    @Override
    public Set<Pool> getPools() {
        return _pools;
    }

    public void resetPools() {
        _pools.clear();
    }

    public void addPool(Pool pool) {
        _pools.add(pool);
    }

    @Override
    public void changePoolMode(Set<String> poolIds, PoolV2Mode poolMode, String userName) throws DAOException {
        for (String id : poolIds) {
            if (getAvailableIds().contains(id)) {
                for (Pool pool : _pools) {
                    if (pool.getName().equals(id)) {
                        pool.getSelectionPool().setPoolMode(poolMode);
                        break;
                    }
                }
            } else {
                throw new IllegalArgumentException(id + " not a preconfigured Pool");
            }
        }
    }

    private Set<String> getAvailableIds() {
        Set<String> ids = new HashSet<>();
        for (Pool pool : _pools) {
            ids.add(pool.getName());
        }
        return ids;
    }

    @Override
    public Set<Pool> getPoolsOfPoolGroup(String poolGroup) throws DAOException {
//        always return all for now
        return _pools;
    }

    @Override
    public Set<SelectionLink> getLinksPointingToPoolGroup(String poolGroup) throws DAOException {
        return Collections.emptySet();
    }

    @Override
    public Set<SelectionLink> getLinks() throws DAOException {
        return Collections.emptySet();
    }

    @Override
    public Set<SelectionPoolGroup> getPoolGroupsOfPool(String poolName) throws DAOException {
        return Collections.emptySet();
    }

    @Override
    public Set<SelectionUnitGroup> getUnitGroups() throws DAOException {
        return Collections.emptySet();
    }

    @Override
    public Set<SelectionUnit> getUnits() throws DAOException {
        return Collections.emptySet();
    }

    @Override
    public Set<SelectionPoolGroup> getPoolGroups() throws DAOException {
        return Collections.emptySet();
    }

    @Override
    public PoolPreferenceLevel[] match(DirectionType type, String netUnitName,
            String protocolUnitName, String hsm,
            String storageClass, String linkGroupName) throws DAOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Partition> getPartitions() throws DAOException {
        return Collections.emptyMap();
    }
}
