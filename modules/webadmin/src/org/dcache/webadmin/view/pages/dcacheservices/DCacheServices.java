package org.dcache.webadmin.view.pages.dcacheservices;

import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.util.CustomLink;

/**
 * Main overview of all dCache-Services
 * @author jans
 */
public class DCacheServices extends AuthenticatedWebPage {

    public DCacheServices() {
        add(new CustomLink("poolListLink", PoolList.class));
    }
}
