package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.webadmin.controller.PoolQueuesService;
import org.dcache.webadmin.controller.exceptions.PoolQueuesServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.PoolQueueBean;

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
            List<PoolQueueBean> poolQueues = new ArrayList<>(pools.size());
            Map<String, List<String>> domainMap = getDomainsDAO().getDomainsMap();

            for (Pool currentPool : pools) {
                PoolQueueBean newPoolQueueBean = createPoolQueueBean(currentPool,
                        domainMap);
                poolQueues.add(newPoolQueueBean);
            }
            _log.debug("returned PoolQueueBeans: " + poolQueues.size());
            Collections.sort(poolQueues);
            return poolQueues;

        } catch (DAOException e) {
            throw new PoolQueuesServiceException(e);
        }

    }

    private PoolQueueBean createPoolQueueBean(Pool pool,
            Map<String, List<String>> domainMap) {
        return BeanDataMapper.poolQueueModelToView(pool, domainMap);
    }

    private DomainsDAO getDomainsDAO() {
        return _daoFactory.getDomainsDAO();
    }

    private PoolsDAO getPoolsDAO() {
        return _daoFactory.getPoolsDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }
}
