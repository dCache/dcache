package org.dcache.webadmin.model.dataaccess.impl;

import dmg.cells.nucleus.CellEndpoint;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.dcache.admin.webadmin.jettycell.JettyCell;
import org.dcache.cells.CellStub;

/**
 * Cell-Stub in Webadmin which is responsible for Cell-Communication
 * @author jans
 */
public class ServletContextCellStub extends CellStub {

    private int _httpsPort;
    private int _httpPort;
    private String _dcacheName = "";

    public ServletContextCellStub(String destination) throws NamingException {
        InitialContext lookupContext = new InitialContext();
        JettyCell jettyCell = (JettyCell) lookupContext.lookup(
                JettyCell.JETTYCELL_NAMING_CONTEXT);
        _httpPort = jettyCell.getHttpPort();
        _httpsPort = jettyCell.getHttpsPort();
        _dcacheName = jettyCell.getDcacheName();
        setCellEndpoint((CellEndpoint) jettyCell);
        setDestination(destination);
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public int getHttpPort() {
        return _httpPort;
    }

    public int getHttpsPort() {
        return _httpsPort;
    }
}
