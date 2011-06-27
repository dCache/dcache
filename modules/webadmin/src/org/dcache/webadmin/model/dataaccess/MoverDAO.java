package org.dcache.webadmin.model.dataaccess;

import java.util.List;
import java.util.Set;
import org.dcache.admin.webadmin.datacollector.datatypes.MoverInfo;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public interface MoverDAO {

    /**
     * @return delivers the active transfers
     */
    public List<MoverInfo> getActiveTransfers() throws DAOException;

    public void killMoversOnSinglePool(Set<Integer> jobids, String targetPool)
            throws DAOException;
}
