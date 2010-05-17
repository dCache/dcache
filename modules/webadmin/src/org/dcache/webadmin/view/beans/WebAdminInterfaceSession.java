package org.dcache.webadmin.view.beans;

import org.apache.wicket.Request;
import org.apache.wicket.protocol.http.WebSession;

/**
 * The session-Object to store Session-Data like User-Credentials
  * @author jans
 */
public class WebAdminInterfaceSession extends WebSession {

     public WebAdminInterfaceSession(Request request) {
        super(request);
    }
}
