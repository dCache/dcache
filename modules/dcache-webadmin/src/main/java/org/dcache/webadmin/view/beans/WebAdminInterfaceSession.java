package org.dcache.webadmin.view.beans;

import org.apache.wicket.Session;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.protocol.http.WebSession;
import org.apache.wicket.request.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The session-Object to store Session-Data like User-Credentials, and page
 * options.
 *
 * @author jans
 * @author arossi
 */
public class WebAdminInterfaceSession extends WebSession {
    private static final long serialVersionUID = -941613160805323716L;
    private static final Logger logger = LoggerFactory.getLogger(WebAdminInterfaceSession.class);

    private final AlarmQueryBean alarmQueryBean = new AlarmQueryBean();
    private final PoolPlotOptionBean poolPlotBean = new PoolPlotOptionBean();

    private UserBean user;

    public static WebAdminInterfaceSession get() {
        return (WebAdminInterfaceSession) Session.get();
    }

    public static boolean hasUserRole(String role) {
        WebAdminInterfaceSession session = get();
        if (!session.isSignedIn()) {
            return false;
        }
        return session.user.hasRole(role);
    }

    public static AlarmQueryBean getAlarmQueryBean() {
        return get().alarmQueryBean;
    }

    public static PoolPlotOptionBean getPoolPlotBean() {
        return get().poolPlotBean;
    }

    public WebAdminInterfaceSession(Request request) {
        super(request);
    }

    public String getUserName() {
        return user.getUsername();
    }

    public boolean hasAnyRole(Roles roles) {
        logger.debug("asking for available roles {}", roles);
        if (!isSignedIn()) {
            return false;
        }
        return user.hasAnyRole(roles);
    }

    public boolean isSignedIn() {
        return user != null;
    }

    public void logoutUser() {
        user = null;
    }

    public void setUser(final UserBean user) {
        this.user = user;
    }
}
