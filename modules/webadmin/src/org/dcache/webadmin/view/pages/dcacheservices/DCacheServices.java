package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.markup.html.basic.Label;
import org.dcache.webadmin.view.pages.basepage.BasePage;

/**
 * Main overview of all dCache-Services
 * @author jans
 */
public class DCacheServices extends BasePage {

    public DCacheServices() {
        add(new Label("dCacheInstanceName",
                getWebadminApplication().getDcacheName()));
    }
}
