package org.dcache.webadmin.model.dataaccess;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public interface DomainsDAO {

    /**
     * To get a map that contains all names of doors, pools, PoolManager, PnfsManager,
     * gPlazma, LoginBroker put into the key which is the domain they are in.
     * @return delivers a map<domainname,List<cellname>>
     */
    Map<String, List<String>> getDomainsMap();

    Set<CellStatus> getCellStatuses();

    /**
     *
     * @param destinations targets for the command
     * @param command command to send
     */
    Set<CellResponse> sendCommand(Set<String> destinations, String command)
            throws DAOException;
}
