package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.view.beans.CellServicesBean;

/**
 *
 * @author jans
 */
public class StandardCellsService implements CellsService {

    private static final Logger _log = LoggerFactory.getLogger(StandardCellsService.class);
    private DAOFactory _daoFactory;

    public StandardCellsService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    @Override
    public List<CellServicesBean> getCellServicesBeans()
    {
        Set<CellStatus> cellStatuses = getDomainsDAO().getCellStatuses();
        _log.debug("returned cellStatuses: {}", cellStatuses.size());
        List<CellServicesBean> cells = new ArrayList<>();

        for (CellStatus currentCell : cellStatuses) {
            CellServicesBean newCellBean = createCellServiceBean(currentCell);
            cells.add(newCellBean);
        }
        _log.debug("returned CellServicesBeans: {}", cells.size());
        Collections.sort(cells);
        return cells;
    }

    private DomainsDAO getDomainsDAO() {
        return _daoFactory.getDomainsDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private CellServicesBean createCellServiceBean(CellStatus currentCell) {
        return BeanDataMapper.cellModelToView(currentCell);
    }
}
