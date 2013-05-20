package org.dcache.webadmin.view;

import org.apache.wicket.Component;
import org.apache.wicket.Page;
import org.apache.wicket.Session;
import org.apache.wicket.authorization.strategies.CompoundAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.page.SimplePageAuthorizationStrategy;
import org.apache.wicket.authroles.authorization.strategies.role.IRoleCheckingStrategy;
import org.apache.wicket.authroles.authorization.strategies.role.RoleAuthorizationStrategy;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.protocol.https.HttpsConfig;
import org.apache.wicket.protocol.https.HttpsMapper;
import org.apache.wicket.request.Request;
import org.apache.wicket.request.Response;
import org.apache.wicket.request.mapper.CryptoMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import dmg.cells.nucleus.CellPath;

import org.dcache.auth.LoginStrategy;
import org.dcache.cells.CellStub;
import org.dcache.services.httpd.HttpServiceCell;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.IAlarmDisplayService;
import org.dcache.webadmin.controller.InfoService;
import org.dcache.webadmin.controller.LinkGroupsService;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.PoolAdminService;
import org.dcache.webadmin.controller.PoolGroupService;
import org.dcache.webadmin.controller.PoolQueuesService;
import org.dcache.webadmin.controller.PoolSelectionSetupService;
import org.dcache.webadmin.controller.PoolSpaceService;
import org.dcache.webadmin.controller.TapeTransfersService;
import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.controller.impl.AlwaysFailLoginService;
import org.dcache.webadmin.controller.impl.LoginStrategyLogInService;
import org.dcache.webadmin.controller.util.ThumbnailPanelProvider;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.activetransfers.ActiveTransfers;
import org.dcache.webadmin.view.pages.alarms.AlarmsPage;
import org.dcache.webadmin.view.pages.billingplots.BillingPlots;
import org.dcache.webadmin.view.pages.celladmin.CellAdmin;
import org.dcache.webadmin.view.pages.cellservices.CellServices;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;
import org.dcache.webadmin.view.pages.info.Info;
import org.dcache.webadmin.view.pages.infoxml.InfoXml;
import org.dcache.webadmin.view.pages.login.LogIn;
import org.dcache.webadmin.view.pages.pooladmin.PoolAdmin;
import org.dcache.webadmin.view.pages.poolgroupview.PoolGroupView;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueuePlots;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueues;
import org.dcache.webadmin.view.pages.poolselectionsetup.PoolSelectionSetup;
import org.dcache.webadmin.view.pages.tapetransferqueue.TapeTransferQueue;
import org.dcache.webadmin.view.pages.unavailable.UnavailablePage;
import org.dcache.webadmin.view.panels.navigation.BasicNavigationPanel;
import org.dcache.webadmin.view.util.Role;

/**
 * This is the main application object for the whole Webadmin-Interface
 *
 * @author jans
 */
public class WebAdminInterface extends WebApplication {

    public static final String MISSING_RESOURCE_KEY = "missing.resource";
    private static final long LOGIN_CELLSTUB_TIMEOUT = 5000;
    private static final List<Class<? extends Component>> ADMIN_PAGES
        = new ArrayList<Class<? extends Component>>(
                    Arrays.asList(PoolAdmin.class, CellAdmin.class,
                                    AlarmsPage.class));
    private static final Logger _log
        = LoggerFactory.getLogger(WebAdminInterface.class);

    private HttpServiceCell _cellEndpoint;
    private LogInService _logInService = new AlwaysFailLoginService();
    private PoolSpaceService _poolSpaceService;
    private PoolQueuesService _poolQueuesService;
    private PoolGroupService _poolGroupService;
    private CellsService _cellsService;
    private InfoService _infoService;
    private PoolAdminService _poolAdminService;
    private CellAdminService _cellAdminService;
    private LinkGroupsService _linkGroupsService;
    private ActiveTransfersService _activeTransfersService;
    private PoolSelectionSetupService _poolSelectionSetupService;
    private TapeTransfersService _tapeTransfersService;
    private IAlarmDisplayService _alarmDisplayService;
    private ThumbnailPanelProvider _thumbnailPanelProvider;
    private String _dcacheName;
    private String _authDestination;
    private Integer _adminGid;
    private String _billingToDb = "no";
    private String _generatePlots = "false";
    private String _poolQueuePlotsEnabled = "false";
    private String _plotsDir;
    private String _exportExt = ".png";
    private int _httpsPort;
    private int _httpPort;

    public ActiveTransfersService getActiveTransfersService() {
        return _activeTransfersService;
    }

    public Integer getAdminGid() {
        return _adminGid;
    }

    public List<Class<? extends Component>> getAdminOnlyPages() {
        return Collections.unmodifiableList(ADMIN_PAGES);
    }

    public IAlarmDisplayService getAlarmDisplayService() {
        return _alarmDisplayService;
    }

    public String getAuthDestination() {
        return _authDestination;
    }

    public String getBillingToDb() {
        return _billingToDb;
    }

    public CellAdminService getCellAdminService() {
        return _cellAdminService;
    }

    public HttpServiceCell getCellEndpoint() {
        return _cellEndpoint;
    }

    public CellsService getCellsService() {
        return _cellsService;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public String getExportExt() {
        return _exportExt;
    }

    public String getGeneratePlots() {
        return _generatePlots;
    }

    @Override
    public Class<? extends Page> getHomePage() {
        return DCacheServices.class;
    }

    public InfoService getInfoService() {
        return _infoService;
    }

    public LinkGroupsService getLinkGroupsService() {
        return _linkGroupsService;
    }

    public LogInService getLogInService() {
        return _logInService;
    }

    public String getPlotsDir() {
        return _plotsDir;
    }

    public PoolAdminService getPoolAdminService() {
        return _poolAdminService;
    }

    public PoolGroupService getPoolGroupService() {
        return _poolGroupService;
    }

    public PoolQueuesService getPoolQueuesService() {
        return _poolQueuesService;
    }

    public PoolSelectionSetupService getPoolSelectionSetupService() {
        return _poolSelectionSetupService;
    }

    public PoolSpaceService getPoolSpaceService() {
        return _poolSpaceService;
    }

    public TapeTransfersService getTapeTransfersService() {
        return _tapeTransfersService;
    }

    public ThumbnailPanelProvider getThumbnailPanelProvider() {
        return _thumbnailPanelProvider;
    }

    public boolean isAuthenticatedMode() {
        return _cellEndpoint.isAuthenticated();
    }

    @Override
    public Session newSession(Request request, Response response) {
        return new WebAdminInterfaceSession(request);
    }

    public void setActiveTransfersService(
                    ActiveTransfersService activeTransfersService) {
        _activeTransfersService = activeTransfersService;
    }

    public void setAdminGid(Integer adminGid) {
        _adminGid = adminGid;
    }

    public void setAlarmDisplayService(IAlarmDisplayService alarmDisplayService) {
        _alarmDisplayService = alarmDisplayService;
    }

    public void setAuthDestination(String authDestination) {
        _authDestination = authDestination;
    }

    public void setBillingToDb(String billingToDb) {
        _billingToDb = billingToDb;
    }

    public void setCellAdminService(CellAdminService cellAdminService) {
        _cellAdminService = cellAdminService;
    }

    public void setCellEndpoint(HttpServiceCell cellEnpoint) {
        _cellEndpoint = cellEnpoint;
        _httpPort = _cellEndpoint.getHttpPort();
        _httpsPort = _cellEndpoint.getHttpsPort();
    }

    public void setCellsService(CellsService cellsService) {
        _cellsService = cellsService;
    }

    public void setDcacheName(String dCacheName) {
        _dcacheName = dCacheName;
    }

    public void setExportExt(String exportExt) {
        _exportExt = exportExt;
    }

    public void setGeneratePlots(String generatePlots) {
        _generatePlots = generatePlots;
    }

    public void setInfoService(InfoService infoService) {
        _infoService = infoService;
    }

    public void setLinkGroupsService(LinkGroupsService linkGroupsService) {
        _linkGroupsService = linkGroupsService;
    }

    public void setLogInService(LogInService logInService) {
        _logInService = logInService;
    }

    public void setPlotsDir(String plotsDir) {
        _plotsDir = plotsDir;
    }

    public void setPoolAdminService(PoolAdminService poolAdminService) {
        _poolAdminService = poolAdminService;
    }

    public void setPoolGroupService(PoolGroupService poolGroupService) {
        _poolGroupService = poolGroupService;
    }

    public void setPoolQueuePlotsEnabled(String enabled) {
        _poolQueuePlotsEnabled = enabled;
    }

    public void setPoolQueuesService(PoolQueuesService poolQueuesService) {
        _poolQueuesService = poolQueuesService;
    }

    public void setPoolSelectionSetupService(
                    PoolSelectionSetupService poolSelectionSetupService) {
        _poolSelectionSetupService = poolSelectionSetupService;
    }

    public void setPoolSpaceService(PoolSpaceService poolSpaceService) {
        _poolSpaceService = poolSpaceService;
    }

    public void setTapeTransfersService(
                    TapeTransfersService tapeTransfersService) {
        _tapeTransfersService = tapeTransfersService;
    }

    public void setThumbnailPanelProvider(ThumbnailPanelProvider provider) {
        _thumbnailPanelProvider = provider;
    }

    @Override
    protected void init() {
        super.init();
        setAuthorizationStrategies();

        getApplicationSettings().setPageExpiredErrorPage(LogIn.class);
        setRootRequestMapper(new CryptoMapper(getRootRequestMapper(), this));

        mountBookmarkablePages();
        markAdminOnlyPages();

        if (isAuthenticatedMode()) {
            setRootRequestMapper(new HttpsMapper(getRootRequestMapper(),
                            new HttpsConfig(_httpPort, _httpsPort)));
        }
    }

    private void markAdminOnlyPages() {
        for (final Class<? extends Component> adminPage : ADMIN_PAGES) {
            MetaDataRoleAuthorizationStrategy.authorize(adminPage, Role.ADMIN);
        }
    }

    private void mountBookmarkablePages() {
        /*
         * warning changes in the pagenames/urls here may affect
         * LegacyForwardHandler in Jettycell - always doublecheck when making
         * changes
         */
        mountPage("login", LogIn.class);
        mountPage("info", Info.class);
        mountPage("cellinfo", CellServices.class);
        mountPage("queueinfo", PoolQueues.class);
        mountPage("usageinfo", PoolList.class);
        mountPage("poolgroups", PoolGroupView.class);
        mountPage("pooladmin", PoolAdmin.class);
        mountPage("celladmin", CellAdmin.class);
        mountPage("infoxml", InfoXml.class);
        mountPage("transfers", ActiveTransfers.class);
        mountPage("poolinfo", PoolSelectionSetup.class);
        mountPage("tapetransfers", TapeTransferQueue.class);
        mountPage("alarms", AlarmsPage.class);

        if (_billingToDb.trim().equalsIgnoreCase("yes")
                        && Boolean.parseBoolean(_generatePlots)) {
            mountPage("billingplots", BillingPlots.class);
            BasicNavigationPanel.addBillingPage();
        } else {
            mountPage("billingplots", UnavailablePage.class);
            BasicNavigationPanel.removeBillingPage();
        }

        if (Boolean.parseBoolean(_poolQueuePlotsEnabled.trim())) {
            mountPage("poolqueueplots", PoolQueuePlots.class);
            BasicNavigationPanel.addPoolQueuePlotsPage();
        } else {
            mountPage("poolqueueplots", UnavailablePage.class);
            BasicNavigationPanel.removePoolQueuePlotsPage();
        }
    }

    private void setAuthorizationStrategies() {
        if (isAuthenticatedMode()) {
            LoginStrategy loginStrategy
                = new RemoteLoginStrategy(new CellStub(_cellEndpoint,
                                          new CellPath(_authDestination),
                                          LOGIN_CELLSTUB_TIMEOUT));
            LoginStrategyLogInService loginService
                = new LoginStrategyLogInService();
            loginService.setLoginStrategy(loginStrategy);
            loginService.setAdminGid(_adminGid);
            _logInService = loginService;
        }

        SimplePageAuthorizationStrategy simplePageStrategy
            = new SimplePageAuthorizationStrategy(AuthenticatedWebPage.class,
                                                  LogIn.class) {

            @Override
            protected boolean isAuthorized() {
                /*
                 * we want the automatic sign-in here,
                 * not on the beforeRender() of the LogIn page as it was;
                 * we also want this to be only for cert signin
                 */
                boolean signedIn
                    = ((WebAdminInterfaceSession) Session.get()).isSignedIn();
                if (!signedIn) {
                    try {
                        LogIn.signInWithCert(_logInService);
                        signedIn = true;
                    } catch (IllegalArgumentException | LogInServiceException e) {
                        _log.debug("could not automatically authorize {}: {}",
                                   "using browser certificate", e.toString());
                    }
                }
                return signedIn;
            }
        };

        RoleAuthorizationStrategy roleStrategy
            = new RoleAuthorizationStrategy(
                        new IRoleCheckingStrategy() {
                            @Override
                            public boolean hasAnyRole(Roles roles) {
                                _log.debug("checking {}", roles.toString());
                                boolean hasAnyRoles = ((WebAdminInterfaceSession)
                                                Session.get()).hasAnyRole(roles);
                                _log.debug("results in: {}", hasAnyRoles);
                                return hasAnyRoles;
                            }
                        });

        final CompoundAuthorizationStrategy compoundStrategy
            = new CompoundAuthorizationStrategy();
        compoundStrategy.add(simplePageStrategy);
        compoundStrategy.add(roleStrategy);
        getSecuritySettings().setAuthorizationStrategy(compoundStrategy);
    }
}
