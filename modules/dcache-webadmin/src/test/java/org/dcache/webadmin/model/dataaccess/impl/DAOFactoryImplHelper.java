package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.LogEntryDAO;
import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;

import static org.mockito.Mockito.mock;

/**
 * Helperclass to instantiate Helper-DAOs for Unittesting
 *
 * @author jans
 */
public class DAOFactoryImplHelper implements DAOFactory {

    PoolsDAOImplHelper _poolsDao = new PoolsDAOImplHelper();
    DomainsDAOHelper _domainsDao = new DomainsDAOHelper();
    LinkGroupsDAOHelper _linkGroupsDao = new LinkGroupsDAOHelper();
    MoverDAO _moverDao = new MoverDAOHelper();

    private LogEntryDAO _logEntryDAO;

    @Override
    public synchronized LogEntryDAO getLogEntryDAO() {
        if (_logEntryDAO == null) {
            _logEntryDAO = mock(LogEntryDAO.class);
        }
        return _logEntryDAO;
    }

    @Override
    public DomainsDAO getDomainsDAO() {
        return _domainsDao;
    }

    @Override
    public InfoDAO getInfoDAO() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public LinkGroupsDAO getLinkGroupsDAO() {
        return _linkGroupsDao;
    }

    @Override
    public MoverDAO getMoverDAO() {
        return _moverDao;
    }

    @Override
    public PoolsDAO getPoolsDAO() {
        return _poolsDao;
    }

    @Override
    public void setDefaultCommandSenderFactory(
                    CommandSenderFactory commandSenderFactory) {
        // meant not to do anything -- feel free to implement later when needed
    }
}
