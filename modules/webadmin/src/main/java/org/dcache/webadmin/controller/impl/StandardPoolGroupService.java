package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dmg.cells.nucleus.CellAddressCore;

import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.webadmin.controller.PoolGroupService;
import org.dcache.webadmin.controller.exceptions.PoolGroupServiceException;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.beans.PoolGroupBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolSpaceBean;

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

    @Override
    public List<PoolGroupBean> getPoolGroups() throws PoolGroupServiceException {
        try {
            Set<String> poolGroupNames = getPoolsDAO().getPoolGroupNames();
            _log.debug("returned poolGroups: {}", poolGroupNames);
            Map<String, List<String>> domainMap = getDomainsDAO().getDomainsMap();

            Set<CellStatus> cellStates = getDomainsDAO().getCellStatuses();

            List<PoolGroupBean> poolGroups = new ArrayList<>();
            for (String currentPoolGroupName : poolGroupNames) {
                PoolGroupBean newPoolGroup = createPoolGroupBean(
                        currentPoolGroupName, domainMap, cellStates);
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

    private DomainsDAO getDomainsDAO() {
        return _daoFactory.getDomainsDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private PoolGroupBean createPoolGroupBean(String poolGroup,
            Map<String, List<String>> domainMap, Set<CellStatus> cellStates)
            throws DAOException {
        List<PoolSpaceBean> poolSpaces = new ArrayList<>();
        List<PoolQueueBean> poolMovers = new ArrayList<>();
        List<CellServicesBean> poolStatuses = new ArrayList<>();

        for (Pool pool : getPoolsDAO().getPoolsOfPoolGroup(poolGroup)) {
            poolSpaces.add(createPoolSpaceBean(pool, domainMap));
            poolMovers.add(createPoolQueueBean(pool, domainMap));
            poolStatuses.add(createCellServiceBean(getMatchingCellStatus(
                    pool, cellStates)));
        }
        PoolGroupBean newPoolGroup = new PoolGroupBean(poolGroup,
                poolSpaces, poolMovers);
        newPoolGroup.setCellStatuses(poolStatuses);
        return newPoolGroup;
    }

    private PoolSpaceBean createPoolSpaceBean(Pool pool,
            Map<String, List<String>> domainMap) {
        return BeanDataMapper.poolModelToView(pool, domainMap);
    }

    private PoolQueueBean createPoolQueueBean(Pool pool,
            Map<String, List<String>> domainMap) {
        return BeanDataMapper.poolQueueModelToView(pool, domainMap);
    }

    private CellServicesBean createCellServiceBean(CellStatus cellStatus) {
        return BeanDataMapper.cellModelToView(cellStatus);
    }

    private CellStatus getMatchingCellStatus(Pool pool, Set<CellStatus> cellStates) {
        CellStatus result = new CellStatus(new CellAddressCore(pool.getName()));
        for (CellStatus cell : cellStates) {
            if (cell.getCellName().equals(pool.getName())) {
                result = cell;
                break;
            }
        }
        return result;
    }
}


