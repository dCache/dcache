package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.ResourceReference;
import org.apache.wicket.behavior.SimpleAttributeModifier;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.image.Image;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.pages.login.LogIn;
import org.dcache.webadmin.view.util.CustomLink;

/**
 * Main overview of all dCache-Services
 * @author jans
 */
public class DCacheServices extends BasePage {

    private static final String AUTHMODE_ONLY_TOOLTIP_MESSAGE = "authmode.only.tooltip";

    public DCacheServices() {
        add(new FeedbackPanel("feedback"));
        add(new Label("dCacheInstanceName",
                getWebadminApplication().getDcacheName()));
        Link login = new CustomLink("loginLink", LogIn.class);
        login.add(new Image("loginImage", new ResourceReference(
                DCacheServices.class, "login.gif")));
        enableOnlyInAuthenticatedMode(login);
        add(login);
        Link logout = new Link("logoutLink") {

            @Override
            public void onClick() {
                if (getWebadminSession().isSignedIn()) {
                    getWebadminSession().logoutUser();
                    info(getStringResource("user.logout"));
                } else {
                    info(getStringResource("user.notLoggedIn"));
                }
            }
        };
        logout.add(new Image("logoutImage", new ResourceReference(
                DCacheServices.class, "logout.gif")));
        enableOnlyInAuthenticatedMode(logout);
        add(logout);
    }

    private void enableOnlyInAuthenticatedMode(Link link) {
        if (!getWebadminApplication().isAuthenticatedMode()) {
            link.setEnabled(false);
            link.add(new SimpleAttributeModifier("title", getStringResource(
                    AUTHMODE_ONLY_TOOLTIP_MESSAGE)));
        }
    }
}
