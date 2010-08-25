package org.dcache.webadmin.model.dataaccess.impl;

import java.util.HashSet;
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

    private Set<NamedCell> _namedCells = new HashSet<NamedCell>();

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
    public Set<CellResponse> sendCommand(Set<String> poolIds, String command) throws DAOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
