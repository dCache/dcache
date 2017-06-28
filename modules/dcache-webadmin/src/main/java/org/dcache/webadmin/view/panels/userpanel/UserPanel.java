package org.dcache.webadmin.view.panels.userpanel;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.Session;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;

import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.LogInLink;

/**
 *
 * This Panel is for displaying the login information for a user.
 * It adds a login link that redirects a user to the login page.
 * When user is logged in it displays the username.
 * The logout link invalidates a session and redirects the user to the home page.
 *
 * @author tanja
 */
public class UserPanel extends BasePanel {
    private static final long serialVersionUID = -4419358909048041100L;

    public UserPanel(String id) {
        super(id);

        WebAdminInterfaceSession session = (WebAdminInterfaceSession) Session.get();

        add(new Label("username", new PropertyModel(this, "session.userName")));
        add(new Link("logout") {

            private static final long serialVersionUID = -7805117496020130503L;

            @Override
            public void onClick() {
                if (session.isSignedIn()) {
                    getSession().invalidate();
                    setResponsePage(DCacheServices.class);
                }
            }

            @Override
            public boolean isVisible() {
                return session.isSignedIn();
            }
        });

        add(new ListView("activeRoles", new PropertyModel(this, "session.sortedRoles")) {
            @Override
            protected void populateItem(ListItem item) {
                Link role = new Link("role", item.getModel()){
                    @Override
                    public void onClick() {
                        session.unassertRole((String)getModelObject());
                    }
                };
                role.setBody(item.getModel());
                role.add(new AttributeModifier("title", Model.of("Active role (click to deactivate).")));
                item.add(role);
            }
        });

        add(new ListView("inactiveRoles", new PropertyModel(this, "session.sortedUnassertedRoles")) {
            @Override
            protected void populateItem(ListItem item) {
                Link role = new Link("role", item.getModel()) {
                    @Override
                    public void onClick() {
                        session.assertRole((String)getModelObject());
                    }
                };
                role.setBody(item.getModel());
                role.add(new AttributeModifier("title", Model.of("Inactive role (click to activate).")));
                item.add(role);
            }
        });

        add(new LogInLink("login") {

            private static final long serialVersionUID = 6704578675572299011L;

            @Override
            public boolean isVisible() {
                return !session.isSignedIn();
            }
        });
    }
}
