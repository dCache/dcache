package org.dcache.webadmin.view.pages;

import org.apache.wicket.Request;
import org.apache.wicket.Response;
import org.apache.wicket.Session;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.UserBean;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.util.Role;

/**
 * Helper to construct the Application for tests
 * @author jans
 */
public class ApplicationFactoryHelper {

    public static WebAdminInterface createSignedInAsAdminApplication() {
        WebAdminInterface application = new WebAdminInterface() {

            @Override
            public String getConfigurationType() {
                return DEPLOYMENT;
            }

            @Override
            public Session newSession(Request request,
                    Response response) {
                WebAdminInterfaceSession session = new WebAdminInterfaceSession(request);
                UserBean user = new UserBean();
                user.setUsername("admin");
                user.addRole(Role.ADMIN);
                session.setUser(user);
                return session;
            }
        };

        return application;
    }

    public static WebAdminInterface createSignedInAsGuestApplication() {
        WebAdminInterface application = new WebAdminInterface() {

            @Override
            public String getConfigurationType() {
                return DEPLOYMENT;
            }

            @Override
            public Session newSession(Request request,
                    Response response) {
                WebAdminInterfaceSession session = new WebAdminInterfaceSession(request);
                UserBean user = new UserBean();
                user.setUsername("Guest");
                user.addRole(Role.GUEST);
                session.setUser(user);
                return session;
            }
        };

        return application;
    }

    public static WebAdminInterface createNotSignedInApplication() {
        WebAdminInterface application = new WebAdminInterface() {

            @Override
            public String getConfigurationType() {
                return DEPLOYMENT;
            }

            @Override
            public Session newSession(
                    Request request,
                    Response response) {
                return new WebAdminInterfaceSession(request) {

                    @Override
                    public boolean isSignedIn() {
                        return false;
                    }
                };
            }
        };

        return application;
    }

    public static WebAdminInterface createStandardApplication() {
        WebAdminInterface application = new WebAdminInterface() {

            @Override
            public String getConfigurationType() {
                return DEPLOYMENT;
            }
        };

        return application;
    }
}
