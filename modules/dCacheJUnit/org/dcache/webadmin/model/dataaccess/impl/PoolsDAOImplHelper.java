package org.dcache.webadmin.model.dataaccess.impl;

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

    public Set<Pool> getPools() throws DAOException {
        return _pools;
    }

    public void resetPools() {
        _pools.clear();
    }

    public Set<NamedCell> getNamedCells() throws DAOException {
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
}
