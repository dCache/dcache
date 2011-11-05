package org.dcache.webadmin.model.dataaccess.impl;

import diskCacheV111.pools.PoolV2Mode;
import java.util.Set;
import java.util.HashSet;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * This class is the PoolsDAO for most unit tests, so that there is no need to
 * have a running dcache to run the Unittests. It is possible to add customized
 * pools without having to manipulate the whole XML like it would be, if the
 * XMLDataGathererHelper is used in combination with the "real" DAO.
 * @author jans
 */
public class PoolsDAOImplHelper implements PoolsDAO {

    private HashSet<String> _poolGroups = new HashSet<String>();
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
        Set<String> ids = new HashSet<String>();
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
}
