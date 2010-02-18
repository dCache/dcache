package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.link.Link;

/**
 *
 * @author jans
 */
public class DCacheServices extends WebPage {

    public DCacheServices() {
        add(new Link("poolListLink") {

            @Override
            public void onClick() {
            }
        });
    }
}
