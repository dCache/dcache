package org.dcache.webadmin.model.dataaccess;

import java.util.Set;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public interface DomainsDAO {

    public Set<CellStatus> getCellStatuses() throws DAOException;

    /**
     *
     * @return delivers a list of Named Cells in dCache
     */
    public Set<NamedCell> getNamedCells() throws DAOException;

    /**
     *
     * @param destinations targets for the command
     * @param command command to send
     */
    public Set<CellResponse> sendCommand(Set<String> destinations, String command)
            throws DAOException;
}
