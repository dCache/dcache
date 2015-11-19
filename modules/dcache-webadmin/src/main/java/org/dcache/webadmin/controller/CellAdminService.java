package org.dcache.webadmin.controller;

import java.util.List;
import java.util.Map;

import org.dcache.webadmin.controller.exceptions.CellAdminServiceException;

/**
 *
 * @author jans
 */
public interface CellAdminService {

    Map<String, List<String>> getDomainMap()
            throws CellAdminServiceException;

    /**
     * @param target is the targeted Cell. Can eigther be Name of a well-known
     * cell (e.g. a pool) or include the domain for other cells (cell@domain)
     * @param command the command to send to the targeted cell. Available
     * commands depend on the targeted cell
     */
    String sendCommand(String target, String command)
            throws CellAdminServiceException;
}
