package org.dcache.webadmin.model.dataaccess.impl;

import diskCacheV111.pools.PoolV2Mode;
import java.util.Set;
import java.util.HashSet;
import org.dcache.webadmin.model.businessobjects.NamedCell;
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

    private Set<Pool> _pools = new HashSet();
    private Set<NamedCell> _namedCell = new HashSet();

    public PoolsDAOImplHelper() {
        _pools = XMLDataGathererHelper.getExpectedPools();
        _namedCell = XMLDataGathererHelper.getExpectedNamedCells();
    }

    @Override
    public Set<Pool> getPools() {
        return _pools;
    }

    public void resetPools() {
        _pools.clear();
    }

    @Override
    public Set<NamedCell> getNamedCells() {
        return _namedCell;
    }

    public void resetNamedCells() {
        _namedCell.clear();
    }

    public void addNamedCell(NamedCell namedCell) {
        _namedCell.add(namedCell);
    }

    public void addPool(Pool pool) {
        _pools.add(pool);
    }

    @Override
    public void changePoolMode(Set<String> poolIds, PoolV2Mode poolMode, String userName) throws DAOException {
        boolean enabled = isEnabled(poolMode);
        for (String id : poolIds) {
            if (getAvailableIds().contains(id)) {
                for (Pool pool : _pools) {
                    if (pool.getName().equals(id)) {
                        pool.setEnabled(enabled);
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

    private boolean isEnabled(PoolV2Mode poolMode) {
        return (poolMode.getMode() != PoolV2Mode.DISABLED_RDONLY &&
                poolMode.getMode() != PoolV2Mode.DISABLED_STRICT);
    }
}
