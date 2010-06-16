package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for the DAOs. The whole design with an factory is mainly
 * introduced for better testablility with Unittests
 * @author jans
 */
public class DAOFactoryImpl implements DAOFactory {

    private Logger _log = LoggerFactory.getLogger(DAOFactory.class);
    private CommandSenderFactory _defaultCommandSenderFactory;

    @Override
    public PoolsDAO getPoolsDAO() {
        _log.debug("PoolsDAO requested");
        if (_defaultCommandSenderFactory == null) {
            throw new IllegalStateException("DefaultPoolCommandSender not set");
        }
//      maybe better make it an singleton - they all end up using one cell anyway?
        return new PoolsDAOImpl(_defaultCommandSenderFactory);
    }

    @Override
    public void setDefaultCommandSenderFactory(CommandSenderFactory commandSenderFactory) {
        _log.debug("PoolsDAO commandSenderFactory set {}", commandSenderFactory.toString());
        _defaultCommandSenderFactory = commandSenderFactory;
    }
}
