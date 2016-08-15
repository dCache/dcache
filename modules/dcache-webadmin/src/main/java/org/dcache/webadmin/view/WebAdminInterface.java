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
import org.apache.wicket.core.request.mapper.CryptoMapper;
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.protocol.https.HttpsConfig;
import org.apache.wicket.protocol.https.HttpsMapper;
import org.apache.wicket.request.Request;
import org.apache.wicket.request.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;

import org.dcache.auth.LoginStrategy;
import org.dcache.cells.CellStub;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.AlarmDisplayService;
import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.IBillingService;
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
import org.dcache.webadmin.view.pages.activetransfers.ActiveTransfersPage;
import org.dcache.webadmin.view.pages.alarms.AlarmsPage;
import org.dcache.webadmin.view.pages.billingplots.BillingPlots;
import org.dcache.webadmin.view.pages.celladmin.CellAdmin;
import org.dcache.webadmin.view.pages.cellservices.CellServices;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;
import org.dcache.webadmin.view.pages.login.LogIn;
import org.dcache.webadmin.view.pages.pooladmin.PoolAdmin;
import org.dcache.webadmin.view.pages.poolgroupview.PoolGroupView;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueuePlots;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueues;
import org.dcache.webadmin.view.pages.poolselectionsetup.PoolSelectionSetup;
import org.dcache.webadmin.view.pages.tapetransferqueue.TapeTransferQueue;
import org.dcache.webadmin.view.panels.navigation.BasicNavigationPanel;
import org.dcache.webadmin.view.util.Role;

/**
 * This is the main application object for the whole Webadmin-Interface
 *
 * @author jans
 */
public class WebAdminInterface extends WebApplication implements CellMessageSender
{
    public static final String MISSING_RESOURCE_KEY = "missing.resource";
    private static final long LOGIN_CELLSTUB_TIMEOUT = 5000;
    private static final List<Class<? extends Component>> ADMIN_PAGES
        = new ArrayList<Class<? extends Component>>(
                    Arrays.asList(PoolAdmin.class, CellAdmin.class,
                                    AlarmsPage.class));
    private static final Logger _log
        = LoggerFactory.getLogger(WebAdminInterface.class);

    private CellEndpoint _cellEndpoint;
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
    private IBillingService _billingService;
    private AlarmDisplayService _alarmDisplayService;
    private ThumbnailPanelProvider _thumbnailPanelProvider;
    private String _dcacheName;
    private String _dcacheDescription;
    private String _authDestination;
    private int _adminGid;
    private int _httpsPort;
    private int _httpPort;
    private boolean _generatePlots = false;
    private boolean _poolQueuePlotsEnabled = false;
    private boolean _authenticatedMode = false;

    public ActiveTransfersService getActiveTransfersService() {
        return _activeTransfersService;
    }

    public List<Class<? extends Component>> getAdminOnlyPages() {
        return Collections.unmodifiableList(ADMIN_PAGES);
    }

    public AlarmDisplayService getAlarmDisplayService() {
        return _alarmDisplayService;
    }

    public IBillingService getBillingService() {
        return _billingService;
    }

    public CellAdminService getCellAdminService() {
        return _cellAdminService;
    }

    public CellsService getCellsService() {
        return _cellsService;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public String getDcacheDescription() {
        return _dcacheDescription;
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

    public boolean getAuthenticatedMode() {
        return _authenticatedMode;
    }

    @Override
    public Session newSession(Request request, Response response) {
        return new WebAdminInterfaceSession(request);
    }

    @Required
    public void setActiveTransfersService(
                    ActiveTransfersService activeTransfersService) {
        _activeTransfersService = activeTransfersService;
    }

    @Required
    public void setAdminGid(int adminGid) {
        _adminGid = adminGid;
    }

    @Required
    public void setAlarmDisplayService(AlarmDisplayService alarmDisplayService) {
        _alarmDisplayService = alarmDisplayService;
    }

    @Required
    public void setAuthDestination(String authDestination) {
        _authDestination = authDestination;
    }

    @Required
    public void setAuthenticatedMode(boolean authenticated) {
        _authenticatedMode = authenticated;
    }

    @Required
    public void setBillingService(IBillingService billingService) {
        _billingService = billingService;
    }

    @Required
    public void setCellAdminService(CellAdminService cellAdminService) {
        _cellAdminService = cellAdminService;
    }

    @Required
    public void setHttpPort(int port) {
        _httpPort = port;
    }

    @Required
    public void setHttpsPort(int port) {
        _httpsPort = port;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _cellEndpoint = endpoint;
    }

    @Required
    public void setCellsService(CellsService cellsService) {
        _cellsService = cellsService;
    }

    @Required
    public void setDcacheName(String dCacheName) {
        _dcacheName = dCacheName;
    }

    @Required
    public void setDcacheDescription(String description) {
        _dcacheDescription = description;
    }

    @Required
    public void setGeneratePlots(boolean generatePlots) {
        _generatePlots = generatePlots;
    }

    @Required
    public void setInfoService(InfoService infoService) {
        _infoService = infoService;
    }

    @Required
    public void setLinkGroupsService(LinkGroupsService linkGroupsService) {
        _linkGroupsService = linkGroupsService;
    }

    @Required
    public void setPoolAdminService(PoolAdminService poolAdminService) {
        _poolAdminService = poolAdminService;
    }

    @Required
    public void setPoolGroupService(PoolGroupService poolGroupService) {
        _poolGroupService = poolGroupService;
    }

    @Required
    public void setPoolQueuePlotsEnabled(boolean enabled) {
        _poolQueuePlotsEnabled = enabled;
    }

    @Required
    public void setPoolQueuesService(PoolQueuesService poolQueuesService) {
        _poolQueuesService = poolQueuesService;
    }

    @Required
    public void setPoolSelectionSetupService(
                    PoolSelectionSetupService poolSelectionSetupService) {
        _poolSelectionSetupService = poolSelectionSetupService;
    }

    @Required
    public void setPoolSpaceService(PoolSpaceService poolSpaceService) {
        _poolSpaceService = poolSpaceService;
    }

    @Required
    public void setTapeTransfersService(
                    TapeTransfersService tapeTransfersService) {
        _tapeTransfersService = tapeTransfersService;
    }

    @Required
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

        if (getAuthenticatedMode()) {
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
        mountPage("cellinfo", CellServices.class);
        mountPage("queueinfo", PoolQueues.class);
        mountPage("usageinfo", PoolList.class);
        mountPage("poolgroups", PoolGroupView.class);
        mountPage("pooladmin", PoolAdmin.class);
        mountPage("celladmin", CellAdmin.class);
        mountPage("transfers", ActiveTransfersPage.class);
        mountPage("poolinfo", PoolSelectionSetup.class);
        mountPage("tapetransfers", TapeTransferQueue.class);
        mountPage("alarms", AlarmsPage.class);

        if (_generatePlots) {
            _billingService.initialize();
            mountPage("billingplots", BillingPlots.class);
        } else {
            BasicNavigationPanel.removeBillingPage("The httpd.enable.plots.billing property is false");
        }

        if (_poolQueuePlotsEnabled) {
            mountPage("poolqueueplots", PoolQueuePlots.class);
        } else {
            BasicNavigationPanel.removePoolQueuePlotsPage("The httpd.enable.plots.pool-queue property is false");
        }
    }

    private void setAuthorizationStrategies() {
        if (getAuthenticatedMode()) {
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
                        _log.debug("could not automatically authorize "
                                        + "using browser certificate: {}",
                                        e.toString());
                    }
                }
                return signedIn;
            }
        };

        RoleAuthorizationStrategy roleStrategy
            = new RoleAuthorizationStrategy(
                roles -> {
                    _log.debug("checking {}", roles.toString());
                    boolean hasAnyRoles = ((WebAdminInterfaceSession)
                                    Session.get()).hasAnyRole(roles);
                    _log.debug("results in: {}", hasAnyRoles);
                    return hasAnyRoles;
                });

        final CompoundAuthorizationStrategy compoundStrategy
            = new CompoundAuthorizationStrategy();
        compoundStrategy.add(simplePageStrategy);
        compoundStrategy.add(roleStrategy);
        getSecuritySettings().setAuthorizationStrategy(compoundStrategy);
    }
}
