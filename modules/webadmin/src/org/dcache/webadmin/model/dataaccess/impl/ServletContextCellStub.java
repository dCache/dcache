package org.dcache.webadmin.model.dataaccess.impl;

import dmg.cells.nucleus.CellEndpoint;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.dcache.admin.webadmin.jettycell.JettyCell;
import org.dcache.cells.CellStub;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.auth.KauthFileLoginStrategy;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.UnionLoginStrategy;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.webadmin.controller.impl.AlwaysFailLoginService;
import org.dcache.webadmin.controller.impl.GuestLogInDecorator;
import org.dcache.webadmin.controller.impl.LoginStrategyLogInService;

/**
 * Cell-Stub in Webadmin which is responsible for Cell-Communication
 * and for the transfer of configuration into webadmin
 * (FIXME when a better way to do this is found)
 * @author jans
 */
public class ServletContextCellStub extends CellStub {

    private static final int HTTPSPORT_DUMMY_FOR_NONAUTH_MODE = 8085;
    private static final int ADMINGID_DUMMY_FOR_NONAUTH_MODE = Integer.MAX_VALUE;
    private static final LogInService LOGINSERVICE_DUMMY_FOR_NONAUTH_MODE =
            new AlwaysFailLoginService();
    private boolean _authenticatedMode;
    private int _adminGid = ADMINGID_DUMMY_FOR_NONAUTH_MODE;
    private int _httpsPort = HTTPSPORT_DUMMY_FOR_NONAUTH_MODE;
    private int _httpPort;
    private String _kpwdFilePath = "";
    private String _dcacheName = "";
    private File _kpwdFile;
    private LogInService _logInService = LOGINSERVICE_DUMMY_FOR_NONAUTH_MODE;

    public ServletContextCellStub(String destination) throws NamingException {
        InitialContext lookupContext = new InitialContext();
        JettyCell jettyCell = (JettyCell) lookupContext.lookup(
                JettyCell.JETTYCELL_NAMING_CONTEXT);
        _authenticatedMode = jettyCell.isAuthenticatedMode();
        _httpPort = jettyCell.getHttpPort();
        _dcacheName = jettyCell.getDcacheName();
//      warning true evilness! This is against architecture, because it is a call
//      from a lower architectual level to an upper (model depending on a
//      controller package! But the whole class would be obsolete if all of this
//      dependancy/config injection could be done in Spring and this way this
//      class is the one place with all external packages concentrated in it)
        if (_authenticatedMode && destination.equals("gPlazma")) {
            _httpsPort = jettyCell.getHttpsPort();
            _adminGid = jettyCell.getAdminGid();
            _kpwdFilePath = jettyCell.getKpwdFile();
            initLoginService();
        }
        setCellEndpoint((CellEndpoint) jettyCell);
        setDestination(destination);
    }

    private void initLoginService() {
        UnionLoginStrategy loginStrategy = new UnionLoginStrategy();
        loginStrategy.setAnonymousAccess(UnionLoginStrategy.AccessLevel.NONE);
        loginStrategy.setLoginStrategies(createStrategies());
        LoginStrategyLogInService loginService =
                new LoginStrategyLogInService();
        loginService.setLoginStrategy(loginStrategy);
        loginService.setAdminGid(_adminGid);
        _logInService = new GuestLogInDecorator(loginService);
    }

    private List<LoginStrategy> createStrategies() {
        List<LoginStrategy> loginStrategies = new ArrayList<LoginStrategy>();
        initKpwd();
        loginStrategies.add(new KauthFileLoginStrategy(_kpwdFile));
        loginStrategies.add(new RemoteLoginStrategy(this));
        return loginStrategies;
    }

    private void initKpwd() {
        if ((_kpwdFilePath == null) ||
                (_kpwdFilePath.length() == 0)) {
            throw new IllegalArgumentException(
                    "-kpwd-file file argument wasn't specified correctly");
        } else if (!new File(_kpwdFilePath).exists()) {
            throw new IllegalArgumentException("kpwd file does not exists at " +
                    _kpwdFilePath);
        }
        _kpwdFile = new File(_kpwdFilePath);
    }

    public File getKpwdFile() {
        return _kpwdFile;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public boolean isAuthenticatedMode() {
        return _authenticatedMode;
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

    public LogInService getLogInService() {
        return _logInService;
    }
}
