package org.dcache.webadmin.model.dataaccess;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public interface DomainsDAO {

    /**
     *
     * @return delivers a map<domainname,List<cellname>>
     */
    public Map<String, List<String>> getDomainsMap() throws DAOException;

    public Set<CellStatus> getCellStatuses() throws DAOException;

    /**
     *
     * @param destinations targets for the command
     * @param command command to send
     */
    public Set<CellResponse> sendCommand(Set<String> destinations, String command)
            throws DAOException;
}
