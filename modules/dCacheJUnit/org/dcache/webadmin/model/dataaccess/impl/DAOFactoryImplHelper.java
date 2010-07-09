package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.dataaccess.PoolGroupDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.DAOFactory;

/**
 * Helperclass to instantiate Helper-DAOs for Unittesting
 * @author jans
 */
public class DAOFactoryImplHelper implements DAOFactory {

    PoolsDAOImplHelper _poolsDao = new PoolsDAOImplHelper();
    PoolGroupDAOHelper _poolGroupDao = new PoolGroupDAOHelper();

    @Override
    public PoolsDAO getPoolsDAO() {
        return _poolsDao;
    }

    @Override
    public void setDefaultCommandSenderFactory(CommandSenderFactory commandSenderFactory) {
//  meant not to do anything -- feel free to implement later when needed
    }

    public PoolGroupDAO getPoolGroupDAO() {
        return _poolGroupDao;
    }

    public InfoDAO getInfoDAO() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
