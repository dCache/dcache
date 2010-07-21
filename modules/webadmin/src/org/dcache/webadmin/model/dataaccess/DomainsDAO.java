package org.dcache.webadmin.model.dataaccess;

import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public interface DomainsDAO {

    public Set<CellStatus> getCellStatuses() throws DAOException;
}
