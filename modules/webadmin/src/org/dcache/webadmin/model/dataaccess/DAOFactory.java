package org.dcache.webadmin.model.dataaccess;

import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;

/**
 * The instantiation of DAOs is abstracted this way to make Unittests with
 * Helper DAOs/DAO-Factories possible.
 * @author jan schaefer 29-10-2009
 */
public interface DAOFactory {

    public PoolsDAO getPoolsDAO();

    public void setDefaultCommandSenderFactory(CommandSenderFactory commandSenderFactory);
}
