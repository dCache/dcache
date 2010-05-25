package org.dcache.webadmin.view.pages;

import org.apache.wicket.Request;
import org.apache.wicket.Response;
import org.apache.wicket.Session;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;

/**
 * Helper to construct the Application for tests
 * @author jans
 */
public class ApplicationFactoryHelper {

    public static WebAdminInterface createSignedInApplication() {
        WebAdminInterface application = new WebAdminInterface() {

            @Override
            public String getConfigurationType() {
                return DEPLOYMENT;
            }

            @Override
            public Session newSession(Request request,
                    Response response) {
                return new WebAdminInterfaceSession(request) {

                    @Override
                    public boolean isSignedIn() {
                        return true;
                    }
                };
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
            public Session newSession(Request request,
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
