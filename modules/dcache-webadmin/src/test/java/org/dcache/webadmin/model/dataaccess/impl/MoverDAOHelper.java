package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

/**
 *
 * @author jans
 */
public class MoverDAOHelper implements MoverDAO {

    @Override
    public List<ActiveTransfersBean> getActiveTransfers()
    {
        return Collections.emptyList();
    }

    @Override
    public void killMovers(Iterable<ActiveTransfersBean> jobids) throws DAOException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<RestoreInfo> getRestores()
    {
        return Collections.emptySet();
    }
}
