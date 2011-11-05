package org.dcache.webadmin.model.dataaccess.impl;

import dmg.cells.nucleus.CellEndpoint;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.dcache.admin.webadmin.jettycell.JettyCell;
import org.dcache.cells.CellStub;

/**
 * Cell-Stub in Webadmin which is responsible for Cell-Communication
 * and for the transfer of configuration into webadmin
 * (FIXME when a better way to do this is found)
 * @author jans
 */
public class ServletContextCellStub extends CellStub {

    public ServletContextCellStub(String destination) throws NamingException {
        InitialContext lookupContext = new InitialContext();
        JettyCell jettyCell = (JettyCell) lookupContext.lookup(
                JettyCell.JETTYCELL_NAMING_CONTEXT);
        setCellEndpoint((CellEndpoint) jettyCell);
        setDestination(destination);
    }
}
