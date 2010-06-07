package org.dcache.webadmin.view.pages;

import org.apache.wicket.markup.html.WebPage;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;

/**
 * This class makes a page need authentication to be viewed
 * @author jans
 */
public abstract class AuthenticatedWebPage extends WebPage {

    /*
     * conveniance method since (nearly) every AuthenticatedWebPage will need the
     * session-object to retrive the user
     */
    public WebAdminInterfaceSession getWebadminSession() {
        return (WebAdminInterfaceSession) getSession();
    }
}
