package org.dcache.webadmin.view;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.wicket.Page;
import org.apache.wicket.Request;
import org.apache.wicket.RequestCycle;
import org.apache.wicket.Response;
import org.apache.wicket.Session;
import org.apache.wicket.authorization.strategies.CompoundAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.page.SimplePageAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.role.IRoleCheckingStrategy;
import org.apache.wicket.authorization.strategies.role.RoleAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.role.Roles;
import org.apache.wicket.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.protocol.http.PageExpiredException;
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.protocol.http.WebRequestCycleProcessor;
import org.apache.wicket.protocol.http.request.CryptedUrlWebRequestCodingStrategy;
import org.apache.wicket.protocol.http.request.WebRequestCodingStrategy;
import org.apache.wicket.protocol.https.HttpsConfig;
import org.apache.wicket.protocol.https.HttpsRequestCycleProcessor;
import org.apache.wicket.request.IRequestCodingStrategy;
import org.apache.wicket.request.IRequestCycleProcessor;
import org.dcache.auth.LoginStrategy;
import org.dcache.cells.CellStub;
import org.dcache.services.httpd.HttpServiceCell;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.InfoService;
import org.dcache.webadmin.controller.LinkGroupsService;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.PoolAdminService;
import org.dcache.webadmin.controller.PoolGroupService;
import org.dcache.webadmin.controller.PoolQueuesService;
import org.dcache.webadmin.controller.PoolSelectionSetupService;
import org.dcache.webadmin.controller.PoolSpaceService;
import org.dcache.webadmin.controller.TapeTransfersService;
import org.dcache.webadmin.controller.impl.AlwaysFailLoginService;
import org.dcache.webadmin.controller.impl.GuestLogInDecorator;
import org.dcache.webadmin.controller.impl.LoginStrategyLogInService;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.activetransfers.ActiveTransfers;
import org.dcache.webadmin.view.pages.celladmin.CellAdmin;
import org.dcache.webadmin.view.pages.cellservices.CellServices;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;
import org.dcache.webadmin.view.pages.info.Info;
import org.dcache.webadmin.view.pages.infoxml.InfoXml;
import org.dcache.webadmin.view.pages.login.LogIn;
import org.dcache.webadmin.view.pages.pooladmin.PoolAdmin;
import org.dcache.webadmin.view.pages.poolgroupview.PoolGroupView;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueues;
import org.dcache.webadmin.view.pages.poolselectionsetup.PoolSelectionSetup;
import org.dcache.webadmin.view.pages.tapetransferqueue.TapeTransferQueue;
import org.dcache.webadmin.view.util.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellPath;

/**
 * This is the main application object for the whole Webadmin-Interface
 *
 * @author jans
 */
public class WebAdminInterface extends WebApplication {

    public static final String MISSING_RESOURCE_KEY = "missing.ressource";

    private static final long LOGIN_CELLSTUB_TIMEOUT = 5000;
    private static final List<Class> ADMIN_PAGES = new ArrayList<Class>(
                    Arrays.asList(PoolAdmin.class, CellAdmin.class));
    private static final Logger _log = LoggerFactory.getLogger(WebAdminInterface.class);

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
    private String _dcacheName;
    private String _authDestination;
    private Integer _adminGid;

    public CellsService getCellsService() {
        return _cellsService;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public CellAdminService getCellAdminService() {
        return _cellAdminService;
    }

    public ActiveTransfersService getActiveTransfersService() {
        return _activeTransfersService;
    }

    public List<Class> getAdminOnlyPages() {
        return Collections.unmodifiableList(ADMIN_PAGES);
    }

    private void overwriteRespond(RequestCycle requestCycle) {
        requestCycle.setRedirect(true);
        requestCycle.setResponsePage(LogIn.class);
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

    public void setCellAdminService(CellAdminService cellAdminService) {
        _cellAdminService = cellAdminService;
    }

    public void setAuthDestination(String authDestination) {
        _authDestination = authDestination;
    }

    public void setCellEndpoint(HttpServiceCell cellEnpoint) {
        _cellEndpoint = cellEnpoint;
    }

    public void setCellsService(CellsService cellsService) {
        _cellsService = cellsService;
    }

    public void setDcacheName(String dCacheName) {
        _dcacheName = dCacheName;
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

    public void setPoolAdminService(PoolAdminService poolAdminService) {
        _poolAdminService = poolAdminService;
    }

    public void setPoolGroupService(PoolGroupService poolGroupService) {
        _poolGroupService = poolGroupService;
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

    @Override
    protected void init() {
        super.init();
        setAuthorizationStrategies();
        mountBookmarkablePages();
        markAdminOnlyPages();
    }

    @Override
    protected IRequestCycleProcessor newRequestCycleProcessor() {
        if (isAuthenticatedMode()) {
            return new HttpsRequestCycleProcessor(new HttpsConfig(
                            _cellEndpoint.getHttpPort(),
                            _cellEndpoint.getHttpsPort())) {

                @Override
                protected IRequestCodingStrategy newRequestCodingStrategy() {
                    /*
                     * This is a request coding strategy which encrypts the URL
                     * and hence makes it impossible for users to guess what is
                     * in the url and rebuild it manually
                     */
                    return new CryptedUrlWebRequestCodingStrategy(
                                    new WebRequestCodingStrategy());
                }

                @Override
                public void respond(RuntimeException e, RequestCycle requestCycle) {

                    if (e instanceof PageExpiredException) {
                        overwriteRespond(requestCycle);
                    } else {
                        super.respond(e, requestCycle);
                    }
                }
            };
        } else {
            return new WebRequestCycleProcessor() {

                @Override
                public void respond(RuntimeException e, RequestCycle requestCycle) {

                    if (e instanceof PageExpiredException) {
                        overwriteRespond(requestCycle);
                    } else {
                        super.respond(e, requestCycle);
                    }
                }
            };
        }
    }

    private void markAdminOnlyPages() {
        for (final Class adminPage : ADMIN_PAGES) {
            MetaDataRoleAuthorizationStrategy.authorize(adminPage, Role.ADMIN);
        }
    }

    private void mountBookmarkablePages() {
        /*
         * warning changes in the pagenames/urls here may affect
         * LegacyForwardHandler in Jettycell - always doublecheck when making
         * changes
         */
        mountBookmarkablePage("login", LogIn.class);
        mountBookmarkablePage("info", Info.class);
        mountBookmarkablePage("cellinfo", CellServices.class);
        mountBookmarkablePage("queueinfo", PoolQueues.class);
        mountBookmarkablePage("usageinfo", PoolList.class);
        mountBookmarkablePage("poolgroups", PoolGroupView.class);
        mountBookmarkablePage("pooladmin", PoolAdmin.class);
        mountBookmarkablePage("celladmin", CellAdmin.class);
        mountBookmarkablePage("infoxml", InfoXml.class);
        mountBookmarkablePage("transfers", ActiveTransfers.class);
        mountBookmarkablePage("poolinfo", PoolSelectionSetup.class);
        mountBookmarkablePage("tapetransfers", TapeTransferQueue.class);
    }

    private void setAuthorizationStrategies() {
        if (isAuthenticatedMode()) {
            final LoginStrategy loginStrategy
                = new RemoteLoginStrategy(new CellStub(_cellEndpoint,
                                          new CellPath(_authDestination),
                                          LOGIN_CELLSTUB_TIMEOUT));
            final LoginStrategyLogInService loginService
                = new LoginStrategyLogInService();
            loginService.setLoginStrategy(loginStrategy);
            loginService.setAdminGid(_adminGid);
            _logInService = new GuestLogInDecorator(loginService);
        }

        final SimplePageAuthorizationStrategy simplePageStrategy
            = new SimplePageAuthorizationStrategy(AuthenticatedWebPage.class,
                                                  LogIn.class) {

            @Override
            protected boolean isAuthorized() {
                return ((WebAdminInterfaceSession) Session.get()).isSignedIn();
            }
        };

        final RoleAuthorizationStrategy roleStrategy = new RoleAuthorizationStrategy(
                        new IRoleCheckingStrategy() {
                            @Override
                            public boolean hasAnyRole(Roles roles) {
                                _log.debug("checking {}", roles.toString());
                                final boolean hasAnyRoles
                                    = ((WebAdminInterfaceSession) Session.get())
                                        .hasAnyRole(roles);
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