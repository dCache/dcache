package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import diskCacheV111.pools.PoolV2Mode;

import org.dcache.webadmin.controller.PoolSpaceService;
import org.dcache.webadmin.controller.exceptions.PoolSpaceServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
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
            List<PoolSpaceBean> poolBeans = new ArrayList<>(pools.size());
            Map<String, List<String>> domainMap = getDomainsDAO().getDomainsMap();

            for (Pool currentPool : pools) {
                PoolSpaceBean newPoolBean = createPoolBean(currentPool, domainMap);
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

    private PoolSpaceBean createPoolBean(Pool pool,
            Map<String, List<String>> domainMap) {
        return BeanDataMapper.poolModelToView(pool, domainMap);
    }

    private PoolsDAO getPoolsDAO() {
        return _daoFactory.getPoolsDAO();
    }

    private DomainsDAO getDomainsDAO() {
        return _daoFactory.getDomainsDAO();
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
        for (PoolSpaceBean pool: pools) {
            if (poolIds.contains(pool.getName())) {
                pool.setPoolMode(poolMode);
                /*
                 * deselect the current object for consistency
                 * (since refreshing of the pool objects is not
                 * called immediately after this form submission)
                 */
                pool.setSelected(false);
                pool.setStatePending(true);
            }
        }
    }

    private Set<String> getSelectedIds(List<PoolSpaceBean> pools) {
        Set<String> poolIds = new HashSet<>();
        for (PoolSpaceBean pool : pools) {
            if (pool.isSelected()) {
                poolIds.add(pool.getName());
            }
        }
        return poolIds;
    }
}
