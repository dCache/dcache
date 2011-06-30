package org.dcache.webadmin.view;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.wicket.Page;
import org.apache.wicket.Session;
import org.apache.wicket.Request;
import org.apache.wicket.Response;
import org.apache.wicket.authorization.strategies.CompoundAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.role.Roles;
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.protocol.http.request.CryptedUrlWebRequestCodingStrategy;
import org.apache.wicket.protocol.http.request.WebRequestCodingStrategy;
import org.apache.wicket.protocol.https.HttpsConfig;
import org.apache.wicket.protocol.https.HttpsRequestCycleProcessor;
import org.apache.wicket.request.IRequestCodingStrategy;
import org.apache.wicket.request.IRequestCycleProcessor;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.controller.PoolSpaceService;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;
import org.dcache.webadmin.view.pages.login.LogIn;
import org.apache.wicket.authorization.strategies.page.SimplePageAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.role.IRoleCheckingStrategy;
import org.apache.wicket.authorization.strategies.role.RoleAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.protocol.http.WebRequestCycleProcessor;
import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.InfoService;
import org.dcache.webadmin.controller.LinkGroupsService;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.PoolAdminService;
import org.dcache.webadmin.controller.PoolGroupService;
import org.dcache.webadmin.controller.PoolQueuesService;
import org.dcache.webadmin.view.pages.activetransfers.ActiveTransfers;
import org.dcache.webadmin.view.pages.celladmin.CellAdmin;
import org.dcache.webadmin.view.pages.cellservices.CellServices;
import org.dcache.webadmin.view.pages.info.Info;
import org.dcache.webadmin.view.pages.infoxml.InfoXml;
import org.dcache.webadmin.view.pages.pooladmin.PoolAdmin;
import org.dcache.webadmin.view.pages.poolgroupview.PoolGroupView;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueues;
import org.dcache.webadmin.view.util.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the main application object for the whole Webadmin-Interface
 * @author jans
 */
public class WebAdminInterface extends WebApplication {

    public static final String MISSING_RESOURCE_KEY = "missing.ressource";
    private PoolSpaceService _poolSpaceService;
    private LogInService _logInService;
    private PoolQueuesService _poolQueuesService;
    private PoolGroupService _poolGroupService;
    private CellsService _cellsService;
    private InfoService _infoService;
    private PoolAdminService _poolAdminService;
    private CellAdminService _cellAdminService;
    private LinkGroupsService _linkGroupsService;
    private ActiveTransfersService _activeTransfersService;
    private int _httpPort;
    private int _httpsPort;
    private boolean _authenticatedMode;
    private String _dcacheName;
    private static final List<Class> ADMIN_PAGES = new ArrayList<Class>(Arrays.asList(
            PoolAdmin.class, CellAdmin.class));
    private static final Logger _log = LoggerFactory.getLogger(WebAdminInterface.class);

    @Override
    protected void init() {
        super.init();
        setAuthorizationStrategies();
        mountBookmarkablePages();
        markAdminOnlyPages();
    }

    private void mountBookmarkablePages() {
//      warning changes in the pagenames/urls here may affect
//      LegacyForwardHandler in Jettycell - always doublecheck when making changes
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
    }

    private void markAdminOnlyPages() {
        for (Class adminPage : ADMIN_PAGES) {
            MetaDataRoleAuthorizationStrategy.authorize(adminPage, Role.ADMIN);
        }
    }

    public List<Class> getAdminOnlyPages() {
        return Collections.unmodifiableList(ADMIN_PAGES);
    }

    @Override
    protected IRequestCycleProcessor newRequestCycleProcessor() {
        if (_authenticatedMode) {
            return new HttpsRequestCycleProcessor(new HttpsConfig(_httpPort, _httpsPort)) {

                @Override
                protected IRequestCodingStrategy newRequestCodingStrategy() {
//                This is a request coding strategy which encrypts the URL
//                and hence makes it impossible for users to guess what is
//                in the url and rebuild it manually
                    return new CryptedUrlWebRequestCodingStrategy(new WebRequestCodingStrategy());
                }
            };
        } else {
            return new WebRequestCycleProcessor();
        }

    }

    @Override
    public Session newSession(Request request, Response response) {
        return new WebAdminInterfaceSession(request);
    }

    @Override
    public Class<? extends Page> getHomePage() {
        return DCacheServices.class;
    }

    public void setPoolSpaceService(PoolSpaceService poolSpaceService) {
        _poolSpaceService = poolSpaceService;
    }

    public PoolSpaceService getPoolSpaceService() {
        return _poolSpaceService;
    }

    public void setLogInService(LogInService logInService) {
        _logInService = logInService;
    }

    public LogInService getLogInService() {
        return _logInService;
    }

    public PoolQueuesService getPoolQueuesService() {
        return _poolQueuesService;
    }

    public void setPoolQueuesService(PoolQueuesService poolQueuesService) {
        _poolQueuesService = poolQueuesService;
    }

    public PoolGroupService getPoolGroupService() {
        return _poolGroupService;
    }

    public void setPoolGroupService(PoolGroupService poolGroupService) {
        _poolGroupService = poolGroupService;
    }

    public InfoService getInfoService() {
        return _infoService;
    }

    public void setInfoService(InfoService infoService) {
        _infoService = infoService;
    }

    public CellsService getCellsService() {
        return _cellsService;
    }

    public void setCellsService(CellsService cellsService) {
        _cellsService = cellsService;
    }

    public PoolAdminService getPoolAdminService() {
        return _poolAdminService;
    }

    public void setPoolAdminService(PoolAdminService poolAdminService) {
        _poolAdminService = poolAdminService;
    }

    public CellAdminService getCellAdminService() {
        return _cellAdminService;
    }

    public void setCellAdminService(CellAdminService cellAdminService) {
        _cellAdminService = cellAdminService;
    }

    public LinkGroupsService getLinkGroupsService() {
        return _linkGroupsService;
    }

    public void setLinkGroupsService(LinkGroupsService linkGroupsService) {
        _linkGroupsService = linkGroupsService;
    }

    public ActiveTransfersService getActiveTransfersService() {
        return _activeTransfersService;
    }

    public void setActiveTransfersService(ActiveTransfersService activeTransfersService) {
        _activeTransfersService = activeTransfersService;
    }

    public void setDcacheName(String dCacheName) {
        _dcacheName = dCacheName;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public void setHttpPort(int httpPort) {
        _httpPort = httpPort;
    }

    public void setHttpsPort(int httpsPort) {
        _httpsPort = httpsPort;
    }

    public boolean isAuthenticatedMode() {
        return _authenticatedMode;
    }

    public void setAuthenticatedMode(boolean authenticatedMode) {
        _authenticatedMode = authenticatedMode;
    }

    private void setAuthorizationStrategies() {

        SimplePageAuthorizationStrategy simplePageStrategy = new SimplePageAuthorizationStrategy(
                AuthenticatedWebPage.class, LogIn.class) {

            @Override
            protected boolean isAuthorized() {
                return ((WebAdminInterfaceSession) Session.get()).isSignedIn();
            }
        };

        RoleAuthorizationStrategy roleStrategy = new RoleAuthorizationStrategy(
                new IRoleCheckingStrategy() {

                    @Override
                    public boolean hasAnyRole(Roles roles) {
                        _log.debug("checking {}", roles.toString());
                        boolean hasAnyRoles = ((WebAdminInterfaceSession) Session.get()).hasAnyRole(roles);
                        _log.debug("results in: {}", hasAnyRoles);
                        return hasAnyRoles;
                    }
                });

        CompoundAuthorizationStrategy compoundStrategy = new CompoundAuthorizationStrategy();
        compoundStrategy.add(simplePageStrategy);
        compoundStrategy.add(roleStrategy);
        getSecuritySettings().setAuthorizationStrategy(compoundStrategy);
    }
}
