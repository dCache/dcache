package org.dcache.webadmin.view.pages.poolgroupview;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.apache.wicket.behavior.SimpleAttributeModifier;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.EmptyPanel;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.controller.PoolGroupService;
import org.dcache.webadmin.controller.exceptions.PoolGroupServiceException;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.beans.PoolGroupBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.layout.LayoutHeaderPanel;
import org.dcache.webadmin.view.panels.layout.LayoutItemPanel;
import org.dcache.webadmin.view.panels.poolQueuesPanel.PoolQueuesPanel;
import org.dcache.webadmin.view.panels.poollist.PoolListPanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolGroupView extends BasePage implements AuthenticatedWebPage {

    private Panel EMPTY_PANEL = new EmptyPanel("specialPoolGroupPanel");
    private String SPECIAL_POOLGROUP_HEADER = "specialPoolGroup.header";
    private List<PoolGroupBean> _poolGroups;
    private PoolGroupBean _currentPoolGroup;
    private String _selectedGroup = getStringResource(SPECIAL_POOLGROUP_HEADER);
    private Panel _shownPanel = EMPTY_PANEL;
    private PoolListPanel _poolListPanel = new PoolListPanel(
            "specialPoolGroupPanel", new PropertyModel<List<PoolSpaceBean>>(
            this, "_currentPoolGroup._poolSpaces"), false);
    private PoolQueuesPanel _poolQueuesPanel = new PoolQueuesPanel(
            "specialPoolGroupPanel", new PropertyModel<PoolGroupBean>(
            this, "_currentPoolGroup"));
    private static final Logger _log = LoggerFactory.getLogger(PoolGroupView.class);

    public PoolGroupView() {
        addMarkup();
        getPoolGroupsAction();
    }

    private void addMarkup() {
        Form poolGroups = new Form("poolGroupsForm");
        poolGroups.add(new FeedbackPanel("feedback"));
        poolGroups.add(new LayoutHeaderPanel("layoutHeaderPanel"));
        poolGroups.add(createListview());
        poolGroups.add(new Label("specialPoolGroupHeader",
                new PropertyModel<String>(this, "_selectedGroup")));
        poolGroups.add(new NavigationFragment("miniNavigationFragment"));
        poolGroups.add(_shownPanel);
        add(poolGroups);
    }

    private ListView<PoolGroupBean> createListview() {
        return new ListView<PoolGroupBean>(
                "poolGroupView", new PropertyModel<List<PoolGroupBean>>(
                this, "_poolGroups")) {

            @Override
            protected void populateItem(ListItem<PoolGroupBean> item) {
                final PoolGroupBean poolGroup = item.getModelObject();
                item.add(createLink(poolGroup));
                item.add(new Label("enabled", new Boolean(
                        poolGroup.isEnabled()).toString()));
                item.add(new Label("totalSpace", new Long(
                        poolGroup.getTotalSpace()).toString()));
                item.add(new Label("freeSpace", new Long(
                        poolGroup.getFreeSpace()).toString()));
                item.add(new Label("preciousSpace", new Long(
                        poolGroup.getPreciousSpace()).toString()));
                item.add(new LayoutItemPanel("layoutItemPanel",
                        poolGroup.getPercentagePrecious(), poolGroup.getPercentageUsed(),
                        poolGroup.getPercentageFree()));
            }

            private Link createLink(final PoolGroupBean poolGroup) {
                Link link = new Link("nameLink") {

                    @Override
                    public void onClick() {
                        _selectedGroup = getStringResource(
                                SPECIAL_POOLGROUP_HEADER) + " " + poolGroup.getName();
                        _currentPoolGroup = poolGroup;
                    }
                };
                link.add(new Label("nameMessage", poolGroup.getName()));
                return link;
            }
        };
    }

    private PoolGroupService getPoolGroupService() {
        return getWebadminApplication().getPoolGroupService();
    }

    private void getPoolGroupsAction() {
        try {
            _log.debug("getPoolGroupsAction called");
            setPoolGroups(getPoolGroupService().getPoolGroups());
        } catch (PoolGroupServiceException ex) {
            this.error(getStringResource("error.getPoolgroupsFailed") + ex.getMessage());
            _log.debug("getPoolGroupsAction failed {}", ex.getMessage());
            setPoolGroups(null);
        }
    }

    private void setPoolGroups(List<PoolGroupBean> poolGroups) {
        _poolGroups = poolGroups;
    }

    private class NavigationFragment extends Fragment {

        private Link _link1;
        private Link _link2;
        private Link _link3;

        public NavigationFragment(String id) {
            super(id, "navigationFragment", PoolGroupView.this);
            _link1 = addLink("cellViewLink", "cellViewMessage", EMPTY_PANEL);
            _link2 = addLink("spaceUsageLink", "spaceUsageMessage", _poolListPanel);
            _link3 = addLink("moverViewLink", "moverViewMessage", _poolQueuesPanel);
        }

        private Link addLink(String linkId,
                String MessageId, final Panel panelToShow) {
            Link link = new Link(linkId) {

                @Override
                public void onClick() {
                    removeActiveAttributes();
                    _shownPanel.replaceWith(panelToShow);
                    _shownPanel = panelToShow;
                    addClassAttribute(this, "active");
                }
            };
            link.add(new Label(MessageId,
                    getStringResource(MessageId)));
            add(link);
            return link;
        }

        private void removeActiveAttributes() {
            addClassAttribute(_link1, "");
            addClassAttribute(_link2, "");
            addClassAttribute(_link3, "");
        }

        private void addClassAttribute(WebMarkupContainer container, String value) {
            container.add(new SimpleAttributeModifier("class", value));
        }
    }
}
