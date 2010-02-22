package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.markup.html.WebPage;
import org.dcache.webadmin.view.Util.CustomLink;

/**
 * Main overview of all dCache-Services
 * @author jans
 */
public class DCacheServices extends WebPage {

    public DCacheServices() {
        add(new CustomLink("poolListLink", DCacheServices.class));
    }
}
