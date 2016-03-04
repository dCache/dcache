package org.dcache.webadmin.view.panels.navigation;

import com.google.common.collect.ImmutableList;
import org.apache.wicket.AttributeModifier;
import org.apache.wicket.Component;
import org.apache.wicket.behavior.AttributeAppender;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.StringResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.activetransfers.ActiveTransfersPage;
import org.dcache.webadmin.view.pages.alarms.AlarmsPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.pages.billingplots.BillingPlots;
import org.dcache.webadmin.view.pages.celladmin.CellAdmin;
import org.dcache.webadmin.view.pages.cellservices.CellServices;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;
import org.dcache.webadmin.view.pages.pooladmin.PoolAdmin;
import org.dcache.webadmin.view.pages.poolgroupview.PoolGroupView;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueuePlots;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueues;
import org.dcache.webadmin.view.pages.poolselectionsetup.PoolSelectionSetup;
import org.dcache.webadmin.view.pages.spacetokens.SpaceTokens;
import org.dcache.webadmin.view.pages.tapetransferqueue.TapeTransferQueue;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.Role;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * reusable navigation-panel. Each new link needs to be added to the buildLinks
 * method and a new Property in the BasicNavigationPanel.properties with the
 * correct index behind it (see in the File for the previous ones as examples)
 *
 * @author jans
 */
public class BasicNavigationPanel extends BasePanel {

    private static final long serialVersionUID = 4803403315602047391L;
    private static final Logger _log = LoggerFactory.getLogger(BasicNavigationPanel.class);

    private static final ImmutableList<NavLink> NAVLINKS = navListBuilder().
            add(NavLink.builder().to(DCacheServices.class).withId("home")).
            add(NavLink.builder().to(CellServices.class).withId("cells")).
            add(NavLink.builder().to(PoolList.class).withId("pools")).
            add(NavLink.builder().to(PoolQueues.class).withId("poolqueues")).
            add(NavLink.builder().to(PoolQueuePlots.class).withId("poolqueueplots")).
            add(NavLink.builder().to(PoolGroupView.class).withId("poolgroup")).
            add(NavLink.builder().to(TapeTransferQueue.class).withId("tapetransfers")).
            add(NavLink.builder().to(ActiveTransfersPage.class).withId("activetransfers")).
            add(NavLink.builder().to(BillingPlots.class).withId("billing")).
            add(NavLink.builder().to(PoolSelectionSetup.class).withId("poolselection")).
            add(NavLink.builder().to(PoolAdmin.class).withId("pooladmin")).
            add(NavLink.builder().to(CellAdmin.class).withId("celladmin")).
            add(NavLink.builder().to(SpaceTokens.class).withId("space")).
            add(NavLink.builder().to(AlarmsPage.class).withId("alarms")).
            build();

    private static String billingDisableHint;
    private static String poolQueuePlotsDisableHint;

    /**
     * Information about each navigational link.
     */
    public static class NavLink implements Serializable
    {
        private static final long serialVersionUID = 1L;

        private String _id;
        private Class<? extends BasePage> _page;

        public static NavLink.Builder builder()
        {
            return new NavLink.Builder();
        }

        public String getId()
        {
            return _id;
        }

        public Class<? extends BasePage> getPage()
        {
            return _page;
        }

        public boolean isAdminPage()
        {
            return AuthenticatedWebPage.class.isAssignableFrom(_page);
        }

        public StringResourceModel getLinkText(Component component)
        {
            return new StringResourceModel(_id + ".text", component,
                    null);
        }

        public boolean isLinkTo(Class page)
        {
            return _page.equals(page);
        }

        public static class Builder
        {
            private final NavLink link = new NavLink();

            public NavLink.Builder withId(String id)
            {
                link._id = id;
                return this;
            }

            public NavLink.Builder to(Class<? extends BasePage> page)
            {
                link._page = page;
                return this;
            }

            public NavLink build()
            {
                return link;
            }
        }
    }

    public static NavListBuilder navListBuilder()
    {
        return new NavListBuilder();
    }

    public static class NavListBuilder
    {
        private final ImmutableList.Builder<NavLink> _builder = ImmutableList.builder();

        public ImmutableList<NavLink> build()
        {
            return _builder.build();
        }

        public NavListBuilder add(NavLink.Builder navLink)
        {
            _builder.add(navLink.build());
            return this;
        }
    }

    public BasicNavigationPanel(String id, Class currentPage)
    {
        super(id);
        _log.debug(currentPage.getSimpleName());
        add(new LinkListView("linkList", NAVLINKS, currentPage));
    }

    private class LinkListView extends ListView<NavLink>
    {
        private static final long serialVersionUID = 4665791178375173441L;

        private final Class _currentPage;

        public LinkListView(String id, List<NavLink> items, Class currentPage)
        {
            super(id, items);
            _currentPage = currentPage;
        }

        @Override
        protected void populateItem(ListItem<NavLink> item)
        {
            NavLink navLink = item.getModelObject();

            Link link = new BookmarkablePageLink("link", navLink.getPage());
            link.add(new AttributeModifier("id", "nav." + navLink.getId()));
            link.add(new Label("text", navLink.getLinkText(BasicNavigationPanel.this)));

            Label disabled = new Label("disabled");
            disabled.setVisible(false);

            if (navLink.isLinkTo(BillingPlots.class) && billingDisableHint != null) {
                item.add(new AttributeModifier("title", billingDisableHint));
                item.add(appendCssClass("disabled"));
                disabled = new Label("disabled", navLink.getLinkText(BasicNavigationPanel.this));
                link.setVisible(false);
            }

            if (navLink.isLinkTo(PoolQueuePlots.class) && poolQueuePlotsDisableHint != null) {
                item.add(new AttributeModifier("title", poolQueuePlotsDisableHint));
                item.add(appendCssClass("disabled"));
                disabled = new Label("disabled", navLink.getLinkText(BasicNavigationPanel.this));
                link.setVisible(false);
            }

            if (navLink.isAdminPage() && !WebAdminInterfaceSession.hasUserRole(Role.ADMIN)) {
                item.add(new AttributeModifier("title", getStringResource("tooltip.AdminOnly")));
                item.add(appendCssClass("unauthorized"));
            }

            if (navLink.isLinkTo(_currentPage)) {
                item.add(appendCssClass("active"));
            }

            item.add(link);
            item.add(disabled);
        }
    }

    private static AttributeAppender appendCssClass(String value)
    {
        return new AttributeAppender("class", Model.of(value)).setSeparator(" ");
    }

    public static void removeBillingPage(String hint)
    {
        billingDisableHint = checkNotNull(hint);
    }

    public static void removePoolQueuePlotsPage(String hint)
    {
        poolQueuePlotsDisableHint = checkNotNull(hint);
    }
}
