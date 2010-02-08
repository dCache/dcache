package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.XMLDataGatherer;

/**
 * Helperclass to instantiate Helper-DAOs for Unittesting
 * @author jans
 */
public class DAOFactoryImplHelper implements DAOFactory {

    @Override
    public PoolsDAO getPoolsDAO() {
        return new PoolsDAOImplHelper();
    }

    public void setDefaultXMLDataGatherer(XMLDataGatherer xmlDataGatherer) {
//  meant not to do anything
    }
}
