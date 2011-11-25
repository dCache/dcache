package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.dcache.admin.webadmin.datacollector.datatypes.MoverInfo;
import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public class MoverDAOHelper implements MoverDAO {

    @Override
    public List<MoverInfo> getActiveTransfers() throws DAOException {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void killMoversOnSinglePool(Set<Integer> jobids, String targetPool) throws DAOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<RestoreInfo> getRestores() throws DAOException {
        return Collections.EMPTY_SET;
    }
}
