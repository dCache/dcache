package org.dcache.webadmin.view.pages;

import org.apache.wicket.RuntimeConfigurationType;
import org.apache.wicket.Session;
import org.apache.wicket.request.Request;
import org.apache.wicket.request.Response;

import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.UserBean;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.util.Role;

/**
 * Helper to construct the Application for tests
 *
 * @author jans
 */
public class ApplicationFactoryHelper {

    public static WebAdminInterface createSignedInAsAdminApplication() {
        return new WebAdminInterface() {
            @Override
            protected void init() {
                setAuthDestination("gPlazma");
                setAdminGid(1000);
                super.init();
            }

            @Override
            public boolean getAuthenticatedMode() {
                return true;
            }

            @Override
            public RuntimeConfigurationType getConfigurationType() {
                return RuntimeConfigurationType.DEPLOYMENT;
            }

            @Override
            public Session newSession(Request request, Response response) {
                WebAdminInterfaceSession session = new WebAdminInterfaceSession(
                                request);
                UserBean user = new UserBean();
                user.setUsername("admin");
                user.addRole(Role.ADMIN);
                session.setUser(user);
                return session;
            }
        };
    }

    public static WebAdminInterface createSignedInAsGuestApplication() {
        return new WebAdminInterface() {
            @Override
            public boolean getAuthenticatedMode() {
                return false;
            }

            @Override
            public RuntimeConfigurationType getConfigurationType() {
                return RuntimeConfigurationType.DEPLOYMENT;
            }

            @Override
            public Session newSession(Request request, Response response) {
                WebAdminInterfaceSession session = new WebAdminInterfaceSession(
                                request);
                UserBean user = new UserBean();
                user.setUsername("Guest");
                user.addRole(Role.GUEST);
                session.setUser(user);
                return session;
            }
        };
    }

    public static WebAdminInterface createNotSignedInApplication() {
        return new WebAdminInterface() {
            @Override
            public boolean getAuthenticatedMode() {
                return false;
            }

            @Override
            public RuntimeConfigurationType getConfigurationType() {
                return RuntimeConfigurationType.DEPLOYMENT;
            }

            @Override
            public Session newSession(Request request, Response response) {
                return new WebAdminInterfaceSession(request) {

                    @Override
                    public boolean isSignedIn() {
                        return false;
                    }
                };
            }
        };
    }

    public static WebAdminInterface createStandardApplication() {
        return new WebAdminInterface() {
            @Override
            public boolean getAuthenticatedMode() {
                return false;
            }

            @Override
            public RuntimeConfigurationType getConfigurationType() {
                return RuntimeConfigurationType.DEPLOYMENT;
            }
        };
    }
}
