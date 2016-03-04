package org.dcache.webadmin.controller.impl;

import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.List;

import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.exceptions.ActiveTransfersServiceException;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

public class StandardActiveTransfersService implements ActiveTransfersService
{
    private DAOFactory _daoFactory;

    public StandardActiveTransfersService(DAOFactory daoFactory)
    {
        _daoFactory = daoFactory;
    }

    private MoverDAO getMoverDAO()
    {
        return _daoFactory.getMoverDAO();
    }

    @Override
    public List<ActiveTransfersBean> getTransfers()
    {
        return getMoverDAO().getActiveTransfers();
    }

    @Override
    public void kill(final Collection<ActiveTransfersBean.Key> keys) throws ActiveTransfersServiceException
    {
        try {
            getMoverDAO().killMovers(
                    Iterables.filter(getTransfers(), t -> keys.contains(t.getKey()) && !t.getPool().isEmpty()));
        } catch (DAOException e) {
            throw new ActiveTransfersServiceException(e.getMessage(), e);
        }
    }

    public void setDAOFactory(DAOFactory daoFactory)
    {
        _daoFactory = daoFactory;
    }
}
