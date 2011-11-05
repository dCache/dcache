package org.dcache.webadmin.model.configmapping;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.dcache.admin.webadmin.jettycell.JettyCell;
import org.dcache.auth.KauthFileLoginStrategy;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.UnionLoginStrategy;
import org.dcache.cells.CellStub;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.impl.AlwaysFailLoginService;
import org.dcache.webadmin.controller.impl.GuestLogInDecorator;
import org.dcache.webadmin.controller.impl.LoginStrategyLogInService;

/**
 * Holder for the configuration imported into the webapp. Also instantiates
 * the proper loginservice for depending on authenticated mode usage or not,
 * because this decisiontaking can't be done in Spring as far as I know
 * @author jans
 */
public class ConfigHolder {

    private static final int HTTPSPORT_DUMMY_FOR_NONAUTH_MODE = 8085;
    private static final int ADMINGID_DUMMY_FOR_NONAUTH_MODE = Integer.MAX_VALUE;
    private static final LogInService LOGINSERVICE_DUMMY_FOR_NONAUTH_MODE =
            new AlwaysFailLoginService();
    private static final long LOGIN_CELLSTUB_TIMEOUT = 5000;
    private boolean _authenticatedMode;
    private int _adminGid = ADMINGID_DUMMY_FOR_NONAUTH_MODE;
    private int _httpsPort = HTTPSPORT_DUMMY_FOR_NONAUTH_MODE;
    private int _httpPort;
    private String _kpwdFilePath = "";
    private String _dcacheName = "";
    private File _kpwdFile;
    private LogInService _logInService = LOGINSERVICE_DUMMY_FOR_NONAUTH_MODE;
    private ThreadFactory _threadFactory;
    private String _loginBrokerName = "";
    private String _pnfsManagerName = "";
    private String _poolManagerName = "";
    private String _gplazmaName = "";
    private long _collectorTimeout;
    private long _transfersCollectorUpdate;

    public ConfigHolder() throws NamingException {
        InitialContext lookupContext = new InitialContext();
        JettyCell jettyCell = (JettyCell) lookupContext.lookup(
                JettyCell.JETTYCELL_NAMING_CONTEXT);
        _authenticatedMode = jettyCell.isAuthenticatedMode();
        _httpPort = jettyCell.getHttpPort();
        _dcacheName = jettyCell.getDcacheName();
        _threadFactory = jettyCell.getNucleus();
        _loginBrokerName = jettyCell.getLoginBrokerName();
        _pnfsManagerName = jettyCell.getPnfsManagerName();
        _poolManagerName = jettyCell.getPoolManagerName();
        _gplazmaName = jettyCell.getGPlazmaName();
        _collectorTimeout = jettyCell.getCollectorTimeout();
        _transfersCollectorUpdate = jettyCell.getTransfersCollectorUpdate();
//      warning true evilness! This is against architecture, because it is a call
//      from a lower architectual level to an upper (model depending on a
//      controller package! But the whole class would be obsolete if all of this
//      dependancy/config injection could be done in Spring.
        if (_authenticatedMode) {
            _httpsPort = jettyCell.getHttpsPort();
            _adminGid = jettyCell.getAdminGid();
            _kpwdFilePath = jettyCell.getKpwdFile();
            initLoginService(jettyCell);
        }
    }

    private void initLoginService(JettyCell jettyCell) {
        LoginStrategy loginStrategy;
        if (jettyCell.getGplazmaVersion() == 1) {
            loginStrategy = createStrategyGplazma1(jettyCell);
        } else {
            loginStrategy = createStrategyGplazma2(jettyCell);
        }
        LoginStrategyLogInService loginService = new LoginStrategyLogInService();
        loginService.setLoginStrategy(loginStrategy);
        loginService.setAdminGid(_adminGid);
        _logInService = new GuestLogInDecorator(loginService);
    }

    private LoginStrategy createStrategyGplazma2(JettyCell jettyCell) {
        return new RemoteLoginStrategy(new CellStub((CellEndpoint) jettyCell,
                new CellPath(_gplazmaName), LOGIN_CELLSTUB_TIMEOUT));
    }

    private LoginStrategy createStrategyGplazma1(JettyCell jettyCell) {
        UnionLoginStrategy loginStrategy = new UnionLoginStrategy();
        loginStrategy.setAnonymousAccess(UnionLoginStrategy.AccessLevel.NONE);
        List<LoginStrategy> loginStrategies = new ArrayList<LoginStrategy>();
        initKpwd();
        loginStrategies.add(new KauthFileLoginStrategy(_kpwdFile));
        loginStrategies.add(new RemoteLoginStrategy(new CellStub((CellEndpoint) jettyCell,
                new CellPath(_gplazmaName), LOGIN_CELLSTUB_TIMEOUT)));
        loginStrategy.setLoginStrategies(loginStrategies);
        return loginStrategy;
    }

    private void initKpwd() {
        checkArgument(!Strings.isNullOrEmpty(_kpwdFilePath),
                "kpwd-file file argument wasn't specified correctly");
        checkArgument(new File(_kpwdFilePath).exists(),
                "kpwd file does not exists at " + _kpwdFilePath);
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

    public ThreadFactory getThreadFactory() {
        return _threadFactory;
    }

    public long getCollectorTimeout() {
        return _collectorTimeout;
    }

    public String getLoginBrokerName() {
        return _loginBrokerName;
    }

    public String getPnfsManagerName() {
        return _pnfsManagerName;
    }

    public String getPoolManagerName() {
        return _poolManagerName;
    }

    public String getGplazmaName() {
        return _gplazmaName;
    }

    public long getTransfersCollectorUpdate() {
        return _transfersCollectorUpdate;
    }
}

