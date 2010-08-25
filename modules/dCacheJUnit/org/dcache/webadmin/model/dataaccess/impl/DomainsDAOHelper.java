package org.dcache.webadmin.model.dataaccess.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.businessobjects.NamedCell;

/**
 *
 * @author jans
 */
public class DomainsDAOHelper implements DomainsDAO {

    public static final String RESPONSE_FOR_ALL_RESPONSES = "dummy";
    private Set<NamedCell> _namedCells = new HashSet<NamedCell>();
    private Map<String, List<String>> _domainsMap =
            new HashMap<String, List<String>>();
    private boolean _areAllResponsesFailure = false;
    private boolean _isThrowingExceptionOnCommandSending = false;

    public DomainsDAOHelper() {
        _namedCells = XMLDataGathererHelper.getExpectedNamedCells();
    }

    public void resetNamedCells() {
        _namedCells.clear();
    }

    public void addNamedCell(NamedCell namedCell) {
        _namedCells.add(namedCell);
    }

    @Override
    public Set<CellStatus> getCellStatuses() throws DAOException {
        Set<CellStatus> cellStatuses = new HashSet<CellStatus>();
        return cellStatuses;
    }

    @Override
    public Set<NamedCell> getNamedCells() {
        return _namedCells;
    }

    @Override
    public Map<String, List<String>> getDomainsMap() throws DAOException {
        return _domainsMap;
    }

    @Override
    public Set<CellResponse> sendCommand(Set<String> destinations,
            String command) throws DAOException {
        if (_isThrowingExceptionOnCommandSending) {
            throw new DAOException("exception throwing modus is active");
        }
        Set<CellResponse> responses = new HashSet<CellResponse>();
        for (String destination : destinations) {
            CellResponse newResponse = new CellResponse();
            newResponse.setCellName(destination);
            newResponse.setResponse(RESPONSE_FOR_ALL_RESPONSES);
            newResponse.setIsFailure(_areAllResponsesFailure);
        }
        return responses;
    }

    public void setDomainsMap(Map<String, List<String>> domainsMap) {
        _domainsMap = domainsMap;
    }

    public void setAreAllResponsesFailure(boolean areAllResponsesFailure) {
        _areAllResponsesFailure = areAllResponsesFailure;
    }

    public void setIsThrowingExceptionOnCommandSending(
            boolean isThrowingExceptionOnCommandSending) {
        _isThrowingExceptionOnCommandSending = isThrowingExceptionOnCommandSending;
    }
}
