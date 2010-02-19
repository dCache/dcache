package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.apache.log4j.Logger;
import org.dcache.webadmin.model.dataaccess.XMLDataGatherer;

/**
 * Factory class for the DAOs. The whole design with an factory is mainly
 * introduced for better testablility with Unittests
 * @author jans
 */
public class DAOFactoryImpl implements DAOFactory {

    private Logger _log = Logger.getLogger(DAOFactory.class);
    private XMLDataGatherer _defaultXmlDataGatherer = null;

    public PoolsDAO getPoolsDAO() {
        _log.debug("PoolsDAO requested");
        if (_defaultXmlDataGatherer == null) {
            throw new IllegalStateException("DefaultXmlDataGatherer not set");
        }
//      maybe better make it an singleton - they all end up using one cell anyway?
        return new PoolsDAOImpl(_defaultXmlDataGatherer);
    }

    public void setDefaultXMLDataGatherer(XMLDataGatherer xmlDataGatherer) {
        _log.debug("PoolsDAO xmlDataGatherer set " + xmlDataGatherer.toString());
        _defaultXmlDataGatherer = xmlDataGatherer;
    }
}
