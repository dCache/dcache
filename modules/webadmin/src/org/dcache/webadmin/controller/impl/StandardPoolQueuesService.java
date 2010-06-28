package org.dcache.webadmin.controller.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.controller.PoolQueuesService;
import org.dcache.webadmin.controller.exceptions.PoolQueuesServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.controller.util.NamedCellUtil;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class StandardPoolQueuesService implements PoolQueuesService {

    private static final Logger _log = LoggerFactory.getLogger(StandardPoolQueuesService.class);
    private DAOFactory _daoFactory;

    public StandardPoolQueuesService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    @Override
    public List<PoolQueueBean> getPoolQueues() throws PoolQueuesServiceException {

        try {
            Set<Pool> pools = getPoolsDAO().getPools();
            _log.debug("returned pools: " + pools.size());
            List<PoolQueueBean> poolQueues = new ArrayList<PoolQueueBean>(pools.size());
            Map<String, NamedCell> namedCells = NamedCellUtil.createCellMap(
                    getPoolsDAO().getNamedCells());
            for (Pool currentPool : pools) {
                PoolQueueBean newPoolQueueBean = createPoolQueueBean(currentPool,
                        namedCells);
                poolQueues.add(newPoolQueueBean);
            }
            _log.debug("returned PoolQueueBeans: " + poolQueues.size());
            Collections.sort(poolQueues);
            return poolQueues;

        } catch (DAOException e) {
            throw new PoolQueuesServiceException(e);
        }

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

    private PoolsDAO getPoolsDAO() {
        return _daoFactory.getPoolsDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }
}
