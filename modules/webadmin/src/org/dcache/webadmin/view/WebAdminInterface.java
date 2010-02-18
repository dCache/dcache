package org.dcache.webadmin.view;

import org.apache.wicket.Page;
import org.apache.wicket.protocol.http.WebApplication;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;

/**
 * This is the main application object for the whole Webadmin-Interface
 * @author jans
 */
public class WebAdminInterface extends WebApplication {

    public WebAdminInterface() {
        super();
    }

    @Override
    public Class<? extends Page> getHomePage() {
        return DCacheServices.class;
    }
}
