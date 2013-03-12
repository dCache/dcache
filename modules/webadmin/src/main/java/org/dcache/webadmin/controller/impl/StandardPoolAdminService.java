package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.webadmin.controller.PoolAdminService;
import org.dcache.webadmin.controller.exceptions.PoolAdminServiceException;
import org.dcache.webadmin.controller.util.NamedCellUtil;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.PoolAdminBean;
import org.dcache.webadmin.view.beans.PoolCommandBean;
import org.dcache.webadmin.view.util.SelectableWrapper;

/**
 *
 * @author jans
 */
public class StandardPoolAdminService implements PoolAdminService {

    private static final Logger _log = LoggerFactory.getLogger(
            StandardPoolAdminService.class);
    private DAOFactory _daoFactory;

    public StandardPoolAdminService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    @Override
    public List<PoolAdminBean> getPoolGroups() throws PoolAdminServiceException {
        try {
            Set<String> poolGroups = getPoolsDAO().getPoolGroupNames();
            Map<String, List<String>> domainMap = getDomainsDAO().getDomainsMap();

            List<PoolAdminBean> adminBeans = new ArrayList<>();
            for (String currentPoolGroup : poolGroups) {
                PoolAdminBean newAdmin = createPoolAdminBean(
                        currentPoolGroup, domainMap);
                adminBeans.add(newAdmin);
            }
            return adminBeans;
        } catch (DAOException e) {
            throw new PoolAdminServiceException(e);
        }

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

    private PoolAdminBean createPoolAdminBean(String currentPoolGroup,
            Map<String, List<String>> domainMap) throws DAOException {
        PoolAdminBean newAdmin = new PoolAdminBean(currentPoolGroup);
        List<SelectableWrapper<PoolCommandBean>> groupPools =
                new ArrayList<>();
        for (Pool currentPool : getPoolsDAO().getPoolsOfPoolGroup(currentPoolGroup)) {
            groupPools.add(new SelectableWrapper<>(
                    createPoolCommandBean(currentPool, domainMap)));

        }
        newAdmin.setPools(groupPools);
        return newAdmin;
    }

    private PoolCommandBean createPoolCommandBean(Pool currentPool,
            Map<String, List<String>> domainMap) {
        PoolCommandBean groupPool = new PoolCommandBean();
        groupPool.setName(currentPool.getName());
        groupPool.setDomain(NamedCellUtil.findDomainOfUniqueCell(domainMap,
                currentPool.getName()));
        return groupPool;
    }

    @Override
    public void sendCommand(
            List<SelectableWrapper<PoolCommandBean>> pools, String command)
            throws PoolAdminServiceException {
        Set<String> poolIds = getSelectedPools(pools);
        _log.debug("Sending commnd {} to following pools {}", command, poolIds);
        try {
            Set<CellResponse> responses = getDomainsDAO().sendCommand(poolIds, command);

            for (SelectableWrapper<PoolCommandBean> pool : pools) {
                clearResponse(pool);
                for (CellResponse response : responses) {
                    if (pool.getWrapped().getName().equals(response.getCellName())) {
                        pool.getWrapped().setResponse(response.getResponse());
                    }
                }
            }
        } catch (DAOException e) {
            throw new PoolAdminServiceException(e);
        }
    }

    private Set<String> getSelectedPools(List<SelectableWrapper<PoolCommandBean>> pools) {
        Set<String> poolIds = new HashSet<>();
        for (SelectableWrapper<PoolCommandBean> pool : pools) {
            if (pool.isSelected()) {
                poolIds.add(pool.getWrapped().getName());
            }
        }
        return poolIds;
    }

    private void clearResponse(SelectableWrapper<PoolCommandBean> pool) {
        pool.getWrapped().setResponse("");
    }
}
