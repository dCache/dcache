package org.dcache.webadmin.view;

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
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.PoolGroupService;
import org.dcache.webadmin.controller.PoolQueuesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the main application object for the whole Webadmin-Interface
 * @author jans
 */
public class WebAdminInterface extends WebApplication {

    private PoolSpaceService _poolSpaceService;
    private LogInService _logInService;
    private PoolQueuesService _poolQueuesService;
    private PoolGroupService _poolGroupService;
    private int _httpPort;
    private int _httpsPort;
    private String _dcacheName;
    private static final Logger _log = LoggerFactory.getLogger(WebAdminInterface.class);

    @Override
    protected void init() {
        super.init();
        setAuthorizationStrategies();
    }

    @Override
    protected IRequestCycleProcessor newRequestCycleProcessor() {
        return new HttpsRequestCycleProcessor(new HttpsConfig(_httpPort, _httpsPort)) {

            @Override
            protected IRequestCodingStrategy newRequestCodingStrategy() {
//                This is a request coding strategy which encrypts the URL
//                and hence makes it impossible for users to guess what is
//                in the url and rebuild it manually
                return new CryptedUrlWebRequestCodingStrategy(new WebRequestCodingStrategy());
            }
        };
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
