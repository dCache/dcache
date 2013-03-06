package org.dcache.webadmin.model.dataaccess;

import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * The instantiation of DAOs is abstracted this way to make Unittests with
 * Helper DAOs/DAO-Factories possible.
 * @author jan schaefer 29-10-2009
 */
public interface DAOFactory {

    public DomainsDAO getDomainsDAO();

    public PoolsDAO getPoolsDAO();

    public InfoDAO getInfoDAO();

    public LinkGroupsDAO getLinkGroupsDAO();

    public MoverDAO getMoverDAO();

    public ILogEntryDAO getLogEntryDAO() throws DAOException;

    public void setDefaultCommandSenderFactory(CommandSenderFactory commandSenderFactory);
}
