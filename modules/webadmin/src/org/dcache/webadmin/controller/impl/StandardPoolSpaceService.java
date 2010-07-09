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
import org.dcache.webadmin.controller.PoolSpaceService;
import org.dcache.webadmin.controller.exceptions.PoolSpaceServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.controller.util.NamedCellUtil;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.PoolSpaceBean;

/**
 * Invokes the needed dataaccessmethods and does the mapping
 * between modelobjects and viewobjects.
 * @author jans
 */
public class StandardPoolSpaceService implements PoolSpaceService {

    private static final Logger _log = LoggerFactory.getLogger(StandardPoolSpaceService.class);
    private DAOFactory _daoFactory;

    public StandardPoolSpaceService(DAOFactory DAOFactory) {
        _daoFactory = DAOFactory;
    }

    @Override
    public List<PoolSpaceBean> getPoolBeans() throws PoolSpaceServiceException {
        try {
            Set<Pool> pools = getPoolsDAO().getPools();
            _log.debug("returned pools: " + pools.size());
            List<PoolSpaceBean> poolBeans = new ArrayList<PoolSpaceBean>(pools.size());
            Map<String, NamedCell> namedCells = NamedCellUtil.createCellMap(
                    getPoolsDAO().getNamedCells());
            for (Pool currentPool : pools) {
                PoolSpaceBean newPoolBean = createPoolBean(currentPool, namedCells);
                poolBeans.add(newPoolBean);
            }
            _log.debug("returned PoolBeans: " + poolBeans.size());
            Collections.sort(poolBeans);
            return poolBeans;

        } catch (DAOException e) {
            throw new PoolSpaceServiceException(e);
        }
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private PoolSpaceBean createPoolBean(Pool pool, Map<String, NamedCell> namedCells) {
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
    public void changePoolMode(List<PoolSpaceBean> pools, PoolV2Mode poolMode,
            String userName) throws PoolSpaceServiceException {
        _log.debug("Change Pool mode called: {}", poolMode);
        Set<String> poolIds = getSelectedIds(pools);
        try {
            getPoolsDAO().changePoolMode(poolIds, poolMode, userName);
        } catch (DAOException ex) {
            throw new PoolSpaceServiceException(ex);
        }
    }

    private Set<String> getSelectedIds(List<PoolSpaceBean> pools) {
        Set<String> poolIds = new HashSet<String>();
        for (PoolSpaceBean pool : pools) {
            if (pool.isSelected()) {
                poolIds.add(pool.getName());
            }
        }
        return poolIds;
    }
}
