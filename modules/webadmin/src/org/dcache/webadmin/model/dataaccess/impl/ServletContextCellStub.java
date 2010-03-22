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

    public ServletContextCellStub(String destination) throws NamingException {
        InitialContext lookupContext = new InitialContext();
        CellEndpoint endpoint = (CellEndpoint) lookupContext.lookup(
                JettyCell.JETTYCELL_NAMING_CONTEXT);
        setCellEndpoint(endpoint);
        setDestination(destination);
    }
}
