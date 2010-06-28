package org.dcache.webadmin.controller.impl;

import diskCacheV111.pools.PoolV2Mode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.webadmin.controller.PoolBeanService;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.controller.util.NamedCellUtil;
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

    private static final Logger _log = LoggerFactory.getLogger(PoolBeanServiceImpl.class);
    private DAOFactory _daoFactory;

    public PoolBeanServiceImpl(DAOFactory DAOFactory) {
        _daoFactory = DAOFactory;
    }

    @Override
    public List<PoolBean> getPoolBeans() throws PoolBeanServiceException {
        try {
            Set<Pool> pools = getPoolsDAO().getPools();
            _log.debug("returned pools: " + pools.size());
            List<PoolBean> poolBeans = new ArrayList<PoolBean>(pools.size());
            Map<String, NamedCell> namedCells = NamedCellUtil.createCellMap(
                    getPoolsDAO().getNamedCells());
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

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
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

    private PoolsDAO getPoolsDAO() {
        return _daoFactory.getPoolsDAO();
    }

    @Override
    public void changePoolMode(List<PoolBean> pools, PoolV2Mode poolMode,
            String userName) throws PoolBeanServiceException {
        _log.debug("Change Pool mode called: {}", poolMode);
        Set<String> poolIds = getSelectedIds(pools);
        try {
            getPoolsDAO().changePoolMode(poolIds, poolMode, userName);
        } catch (DAOException ex) {
            throw new PoolBeanServiceException(ex);
        }
    }

    private Set<String> getSelectedIds(List<PoolBean> pools) {
        Set<String> poolIds = new HashSet<String>();
        for (PoolBean pool : pools) {
            if (pool.isSelected()) {
                poolIds.add(pool.getName());
            }
        }
        return poolIds;
    }
}
