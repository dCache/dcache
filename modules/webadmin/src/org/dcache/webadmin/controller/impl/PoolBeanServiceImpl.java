package org.dcache.webadmin.controller.impl;

import diskCacheV111.pools.PoolV2Mode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.webadmin.controller.PoolBeanService;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.PoolBean;

/**
 * Invokes the needed dataaccessmethods and does the mapping
 * between modelobjects and viewobjects.
 * @author jans
 */
public class PoolBeanServiceImpl implements PoolBeanService {

    private static final Logger _log = LoggerFactory.getLogger(PoolBeanServiceImpl.class.getName());
    private DAOFactory _DAOFactory;

    public PoolBeanServiceImpl(DAOFactory DAOFactory) {
        _DAOFactory = DAOFactory;
    }

    public List<PoolBean> getPoolBeans() throws PoolBeanServiceException {
        try {
            Set<Pool> pools = getPoolsDAO().getPools();
            _log.debug("returned pools: " + pools.size());
            List<PoolBean> poolBeans = new ArrayList<PoolBean>(pools.size());
            Map<String, NamedCell> namedCells = createCellMap(getPoolsDAO().getNamedCells());
            for (Pool currentPool : pools) {
                PoolBean newPoolBean = createPoolBean(currentPool, namedCells);
                poolBeans.add(newPoolBean);
            }
            _log.debug("returned PoolBeans: " + poolBeans.size());
            Collections.sort(poolBeans);
            return poolBeans;

        } catch (DAOException e) {
            throw new PoolBeanServiceException(e);
        }
    }

    public void setDAOFactory(DAOFactory DAOFactory) {
        _DAOFactory = DAOFactory;
    }

    private PoolBean createPoolBean(Pool pool, Map<String, NamedCell> namedCells) {
        NamedCell currentNamedCell = namedCells.get(pool.getName());
        if (currentNamedCell != null) {
            return BeanDataMapper.poolModelToView(pool, currentNamedCell);
        }
//        if there is no match for the pool in the namedCells(perhaps
//        not yet available etc.) fill in only the pool
        return BeanDataMapper.poolModelToView(pool);
    }

    private Map<String, NamedCell> createCellMap(Set<NamedCell> namedCells) {
        Map<String, NamedCell> cells = new HashMap();
        for (NamedCell currentNamedCell : namedCells) {
            cells.put(currentNamedCell.getCellName(), currentNamedCell);
        }
        return cells;
    }

    private PoolsDAO getPoolsDAO() {
        return _DAOFactory.getPoolsDAO();
    }

    @Override
    public void changePoolMode(List<PoolBean> pools, PoolV2Mode poolMode, String userName) throws PoolBeanServiceException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
