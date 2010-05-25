package org.dcache.webadmin.view.beans;

import org.apache.wicket.Request;
import org.apache.wicket.protocol.http.WebSession;
import org.apache.wicket.authorization.strategies.role.Roles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The session-Object to store Session-Data like User-Credentials
 * @author jans
 */
public class WebAdminInterfaceSession extends WebSession {

    private String _user;
    private static final Logger _log = LoggerFactory.getLogger(WebAdminInterfaceSession.class);

    public WebAdminInterfaceSession(Request request) {
        super(request);
    }

    public boolean isSignedIn() {
        return _user != null;
    }

    public String getUser() {
        return _user;
    }

    public void setUser(final String user) {
        _user = user;
    }

    public boolean hasAnyRole(Roles roles) {
//        when gPlazma is integrated
//     TODO something like this: return _user.hasAnyRole(roles);
        _log.debug("asking for available roles {}", roles);
        return true;
    }
}
