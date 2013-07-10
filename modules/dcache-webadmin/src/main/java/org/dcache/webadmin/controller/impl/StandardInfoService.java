package org.dcache.webadmin.controller.impl;

import org.dcache.webadmin.controller.InfoService;
import org.dcache.webadmin.controller.exceptions.InfoServiceException;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public class StandardInfoService implements InfoService {

    private DAOFactory _daoFactory;

    public StandardInfoService(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    @Override
    public String getXmlForStatepath(String statepath) throws InfoServiceException {
        try {
            return getInfoDAO().getXmlForStatepath(statepath);
        } catch (DAOException ex) {
            throw new InfoServiceException(ex);
        }
    }

    private InfoDAO getInfoDAO() {
        return _daoFactory.getInfoDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }
}
