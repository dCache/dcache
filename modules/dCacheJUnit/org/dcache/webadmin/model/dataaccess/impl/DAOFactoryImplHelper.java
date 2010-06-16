package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.DAOFactory;

/**
 * Helperclass to instantiate Helper-DAOs for Unittesting
 * @author jans
 */
public class DAOFactoryImplHelper implements DAOFactory {

    PoolsDAOImplHelper _poolsDao = new PoolsDAOImplHelper();

    @Override
    public PoolsDAO getPoolsDAO() {
        return _poolsDao;
    }

    @Override
    public void setDefaultCommandSenderFactory(CommandSenderFactory commandSenderFactory) {
//  meant not to do anything -- feel free to implement later when needed
    }
}
