package org.dcache.webadmin.model.dataaccess;

import java.util.List;
import java.util.Set;

import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

/**
 *
 * @author jans
 */
public interface MoverDAO {

    /**
     * @return delivers the active transfers
     */
    List<ActiveTransfersBean> getActiveTransfers();

    void killMovers(Iterable<ActiveTransfersBean> jobids)
            throws DAOException;

    /**
     * @return delivers the ongoing restores
     */
    Set<RestoreInfo> getRestores();
}
