package org.dcache.webadmin.model.dataaccess.impl;

import dmg.cells.nucleus.CellEndpoint;
import java.io.File;
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

    private int _adminGid;
    private int _httpsPort;
    private int _httpPort;
    private File _kpwdFile;
    private String _dcacheName = "";

    public ServletContextCellStub(String destination) throws NamingException {
        InitialContext lookupContext = new InitialContext();
        JettyCell jettyCell = (JettyCell) lookupContext.lookup(
                JettyCell.JETTYCELL_NAMING_CONTEXT);
        _httpPort = jettyCell.getHttpPort();
        _httpsPort = jettyCell.getHttpsPort();
        _adminGid = jettyCell.getAdminGid();
        _dcacheName = jettyCell.getDcacheName();
        String kpwdFilePath = jettyCell.getKpwdFile();
        if ((kpwdFilePath == null) ||
                (kpwdFilePath.length() == 0) ||
                (!new File(kpwdFilePath).exists())) {
            throw new IllegalArgumentException(
                    "-kpwd-file file argument wasn't specified correctly");
        }
        _kpwdFile = new File(kpwdFilePath);
        setCellEndpoint((CellEndpoint) jettyCell);
        setDestination(destination);
    }

    public File getKpwdFile() {
        return _kpwdFile;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public int getAdminGid() {
        return _adminGid;
    }

    public int getHttpPort() {
        return _httpPort;
    }

    public int getHttpsPort() {
        return _httpsPort;
    }
}
