package org.dcache.webadmin.model.dataaccess.communication.collectors;

import java.util.Map;
import org.dcache.cells.CellStub;

/**
 * A collector is a runnable that can be run to collect information with cell-
 * communication via its cellstub and put it into its pagecache to later
 * deliver information to a webpage via DAOs. Each implementation should use a
 * ContextPath of the corresponding constant-interface ContextPaths as key
 * to this information.
 * @author jans
 */
public abstract class Collector implements Runnable {

    protected String _name = "";
    protected Map<String, Object> _pageCache;
    protected CellStub _cellStub;

    public void setCellStub(CellStub cellstub) {
        _cellStub = cellstub;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public void setPageCache(Map<String, Object> pageCache) {
        _pageCache = pageCache;
    }

    public Map<String, Object> getPageCache() {
        return _pageCache;
    }
}
