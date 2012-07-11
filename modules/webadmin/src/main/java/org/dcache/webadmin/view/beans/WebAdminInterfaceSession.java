package org.dcache.webadmin.view.beans;

import org.apache.wicket.Session;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.protocol.http.WebSession;
import org.apache.wicket.request.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The session-Object to store Session-Data like User-Credentials
 * @author jans
 */
public class WebAdminInterfaceSession extends WebSession {

    private UserBean _user;
    private static final Logger _log = LoggerFactory.getLogger(WebAdminInterfaceSession.class);

     public WebAdminInterfaceSession(Request request) {
        super(request);
    }

    public static WebAdminInterfaceSession get() {
        return (WebAdminInterfaceSession) Session.get();
    }

    public boolean isSignedIn() {
        return _user != null;
    }

    public String getUserName() {
        return _user.getUsername();
    }

    public void setUser(final UserBean user) {
        _user = user;
    }

    public void logoutUser() {
        _user = null;
    }

    public boolean hasAnyRole(Roles roles) {
        _log.debug("asking for available roles {}", roles);
        if (!isSignedIn()) {
            return false;
        }
        return _user.hasAnyRole(roles);
    }
}
