package org.dcache.webadmin.view.panels.navigation;

import com.google.common.collect.Lists;
import org.apache.wicket.AttributeModifier;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import org.dcache.webadmin.view.pages.activetransfers.ActiveTransfers;
import org.dcache.webadmin.view.pages.alarms.AlarmsPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.pages.billingplots.BillingPlots;
import org.dcache.webadmin.view.pages.celladmin.CellAdmin;
import org.dcache.webadmin.view.pages.cellservices.CellServices;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;
import org.dcache.webadmin.view.pages.infoxml.InfoXml;
import org.dcache.webadmin.view.pages.pooladmin.PoolAdmin;
import org.dcache.webadmin.view.pages.poolgroupview.PoolGroupView;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueuePlots;
import org.dcache.webadmin.view.pages.poolqueues.PoolQueues;
import org.dcache.webadmin.view.pages.poolselectionsetup.PoolSelectionSetup;
import org.dcache.webadmin.view.pages.spacetokens.SpaceTokens;
import org.dcache.webadmin.view.pages.tapetransferqueue.TapeTransferQueue;
import org.dcache.webadmin.view.pages.unavailable.UnavailablePage;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.Role;

/**
 * reusable navigation-panel. Each new link needs to be added to the buildLinks
 * method and a new Property in the BasicNavigationPanel.properties with the
 * correct index behind it (see in the File for the previous ones as examples)
 *
 * @author jans
 */
public class BasicNavigationPanel extends BasePanel {

    private static final long serialVersionUID = 4803403315602047391L;
    private Class _currentPage;
    private static final String LINK_TITLE_PROPERTY_NAME = "link";
    private static List<Class<? extends BasePage>> _linkList = Lists.newArrayList(
                    DCacheServices.class, CellServices.class, PoolList.class,
                    PoolQueues.class, UnavailablePage.class, PoolGroupView.class,
                    TapeTransferQueue.class, ActiveTransfers.class, UnavailablePage.class,
                    PoolSelectionSetup.class, PoolAdmin.class, CellAdmin.class,
                    SpaceTokens.class, InfoXml.class, AlarmsPage.class);
    private static final Logger _log = LoggerFactory.getLogger(BasicNavigationPanel.class);

    public BasicNavigationPanel(String id, Class currentPage) {
        super(id);
        _currentPage = currentPage;
        _log.debug(currentPage.getSimpleName());
        add(new LinkListView<Class>("linkList", _linkList));
    }

    private class LinkListView<T> extends ListView<T> {

        private static final long serialVersionUID = 4665791178375173441L;

        public LinkListView(String id, List<? extends T> items) {
            super(id, items);
        }

        @Override
        protected void populateItem(ListItem item) {
            Class targetPage = (Class) item.getModelObject();
            BookmarkablePageLink link = new BookmarkablePageLink("link",
                            targetPage);
            handleAdminPage(targetPage, item);
            setLinkTitle(link, item.getIndex());
            handleActivePage(targetPage, item);
            item.add(link);
        }

        private void handleAdminPage(Class targetPage, ListItem item) {
            if (isAdminPage(targetPage)) {
                if (!isUserAdmin()) {
                    addAdminOnlyTooltip(item);
                }
            }
        }

        private boolean isAdminPage(Class targetPage) {
            return ((BasePage) getPage()).getWebadminApplication().
                    getAdminOnlyPages().contains(targetPage);
        }

        private void addAdminOnlyTooltip(ListItem item) {
            item.add(new AttributeModifier("title", getStringResource(
                    "tooltip.AdminOnly").getObject()));
        }

        private boolean isUserAdmin() {
            return ((BasePage) getPage()).getWebadminSession().
                    hasAnyRole(new Roles(Role.ADMIN));
        }

        private void setLinkTitle(BookmarkablePageLink link, int linkNumber) {
            link.add(new Label("linkMessage", getStringResource(
                    LINK_TITLE_PROPERTY_NAME + linkNumber)));
        }

        private void handleActivePage(Class targetPage, ListItem item) {
            if (isActivePage(targetPage)) {
                addActiveAttribute(item);
            }
        }

        private boolean isActivePage(Class targetPage) {
            return targetPage.equals(_currentPage);
        }

        private void addActiveAttribute(ListItem item) {
            item.add(new AttributeModifier("class", "active"));
        }
    }

    public static void addBillingPage() {
        int i = _linkList.indexOf(ActiveTransfers.class);
        _linkList.set(i+1, BillingPlots.class);
    }

    public static void removeBillingPage() {
        int i = _linkList.indexOf(ActiveTransfers.class);
        _linkList.set(i+1, UnavailablePage.class);
    }

    public static void addPoolQueuePlotsPage() {
        int i = _linkList.indexOf(PoolQueues.class);
        _linkList.set(i+1, PoolQueuePlots.class);
    }

    public static void removePoolQueuePlotsPage() {
        int i = _linkList.indexOf(PoolQueues.class);
        _linkList.set(i+1, UnavailablePage.class);
    }
}
