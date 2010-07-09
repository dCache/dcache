package org.dcache.webadmin.controller.impl;

import java.util.ArrayList;
import java.util.List;
import org.dcache.webadmin.controller.PoolGroupService;
import org.dcache.webadmin.controller.exceptions.PoolGroupServiceException;
import org.dcache.webadmin.view.beans.PoolGroupBean;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.controller.util.NamedCellUtil;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.PoolGroupDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class StandardPoolGroupService implements PoolGroupService {

    private static final Logger _log = LoggerFactory.getLogger(StandardPoolQueuesService.class);
    private DAOFactory _daoFactory;

    public StandardPoolGroupService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    public List<PoolGroupBean> getPoolGroups() throws PoolGroupServiceException {
        try {
            Set<Pool> pools = getPoolsDAO().getPools();
            Set<String> poolGroupNames = getPoolGroupDAO().getPoolGroupNames();
            _log.debug("returned pools: {} returned poolGroups: {}", pools.size(),
                    poolGroupNames.size());
            Map<String, NamedCell> namedCells = NamedCellUtil.createCellMap(
                    getPoolsDAO().getNamedCells());

            List<PoolGroupBean> poolGroups = new ArrayList<PoolGroupBean>();
            for (String currentPoolGroupName : poolGroupNames) {
                PoolGroupBean newPoolGroup = createPoolGroupBean(
                        currentPoolGroupName, pools, namedCells);
                poolGroups.add(newPoolGroup);
            }
            _log.debug("returned PoolGroupBeans: " + poolGroups.size());
            Collections.sort(poolGroups);
            return poolGroups;
        } catch (DAOException e) {
            throw new PoolGroupServiceException(e);
        }


    }

    private PoolsDAO getPoolsDAO() {
        return _daoFactory.getPoolsDAO();
    }

    private PoolGroupDAO getPoolGroupDAO() {
        return _daoFactory.getPoolGroupDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private PoolGroupBean createPoolGroupBean(String currentPoolGroupName,
            Set<Pool> pools, Map<String, NamedCell> namedCells) {
        List<PoolSpaceBean> poolSpaces = new ArrayList<PoolSpaceBean>();
        List<PoolQueueBean> poolMovers = new ArrayList<PoolQueueBean>();
        for (Pool currentPool : pools) {
            if (currentPool.isInPoolGroup(currentPoolGroupName)) {
                poolSpaces.add(createPoolSpaceBean(currentPool, namedCells));
                poolMovers.add(createPoolQueueBean(currentPool, namedCells));
            }
        }
        PoolGroupBean newPoolGroup = new PoolGroupBean(currentPoolGroupName,
                poolSpaces, poolMovers);
        return newPoolGroup;
    }

    private PoolSpaceBean createPoolSpaceBean(Pool pool, Map<String, NamedCell> namedCells) {
        NamedCell currentNamedCell = namedCells.get(pool.getName());
        if (currentNamedCell != null) {
            return BeanDataMapper.poolModelToView(pool, currentNamedCell);
        }
//        if there is no match for the pool in the namedCells(perhaps
//        not yet available etc.) fill in only the pool
        return BeanDataMapper.poolModelToView(pool);
    }

    private PoolQueueBean createPoolQueueBean(Pool pool, Map<String, NamedCell> namedCells) {
        NamedCell currentNamedCell = namedCells.get(pool.getName());
        if (currentNamedCell != null) {
            return BeanDataMapper.poolQueueModelToView(pool, currentNamedCell);
        }
//        if there is no match for the pool in the namedCells(perhaps
//        not yet available etc.) fill in only the pool
        return BeanDataMapper.poolQueueModelToView(pool);
    }
}


