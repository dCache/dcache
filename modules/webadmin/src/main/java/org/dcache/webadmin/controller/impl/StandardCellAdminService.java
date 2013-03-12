package org.dcache.webadmin.controller.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.exceptions.CellAdminServiceException;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public class StandardCellAdminService implements CellAdminService {

    private static final Logger _log = LoggerFactory.getLogger(
            StandardCellAdminService.class);
    private DAOFactory _daoFactory;

    public StandardCellAdminService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    @Override
    public Map<String, List<String>> getDomainMap()
    {
        return getDomainsDAO().getDomainsMap();
    }

    @Override
    public String sendCommand(String target, String command)
            throws CellAdminServiceException {
        _log.debug("Sending command {} to cell {}", command, target);
        try {
            Set<String> targets = new HashSet<>();
            targets.add(target);
            Set<CellResponse> responses = getDomainsDAO().sendCommand(targets, command);
            CellResponse response = new CellResponse();
            if (responses.iterator().hasNext()) {
                response = responses.iterator().next();
            }
            return response.getResponse();
        } catch (DAOException e) {
            throw new CellAdminServiceException(e);
        }
    }

    private DomainsDAO getDomainsDAO() {
        return _daoFactory.getDomainsDAO();
    }
}
