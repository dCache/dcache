package org.dcache.webadmin.model.dataaccess.impl;

import java.util.HashSet;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public class DomainsDAOHelper implements DomainsDAO {

    @Override
    public Set<CellStatus> getCellStatuses() throws DAOException {
        Set<CellStatus> cellStatuses = new HashSet<CellStatus>();
        return cellStatuses;
    }
}
