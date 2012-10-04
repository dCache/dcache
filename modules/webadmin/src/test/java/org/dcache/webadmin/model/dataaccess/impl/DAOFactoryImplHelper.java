package org.dcache.webadmin.model.dataaccess.impl;

import static org.mockito.Mockito.mock;

import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.IAlarmDAO;
import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.exceptions.DAOException;

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

    private IAlarmDAO _alarmDAO;

    @Override
    public synchronized IAlarmDAO getAlarmDAO() throws DAOException {
        if (_alarmDAO == null) {
            _alarmDAO = mock(IAlarmDAO.class);
        }
        return _alarmDAO;
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
