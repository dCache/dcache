package org.dcache.webadmin.view.panels.poollist;

import org.apache.wicket.AttributeModifier;

import org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;

import java.util.List;

import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.pages.poollist.PoolList;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.panels.layout.LayoutHeaderPanel;
import org.dcache.webadmin.view.panels.layout.LayoutItemPanel;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.dcache.webadmin.view.util.Role;

/**
 * Reusable Panel for the Listview on Poolspaces
 *
 * @author jans
 */
public class PoolListPanel extends BasePanel {

    private static final long serialVersionUID = 8191342980744557065L;
    private boolean _showCheckbox;
    private PoolList _poolList;

    public PoolListPanel(String id, IModel<? extends List<PoolSpaceBean>> model,
            boolean showCheckbox) {
        super(id);
        _showCheckbox = showCheckbox;
        Label selectBoxHeaderLabel = new Label("selectBoxHeader",
                getStringResource("PoolPanel.selected.header"));
        selectBoxHeaderLabel.setVisibilityAllowed(_showCheckbox);
        MetaDataRoleAuthorizationStrategy.authorize(selectBoxHeaderLabel,
                RENDER, Role.ADMIN);
        add(selectBoxHeaderLabel);
        add(new LayoutHeaderPanel("PoolPanel.layoutHeaderPanel"));
        add(new PoolBeanListView("poolPanelListview", model));
    }

    private class PoolBeanListView extends EvenOddListView<PoolSpaceBean> {

        private static final long serialVersionUID = 5738279825189426773L;

        PoolBeanListView(String id, IModel<? extends List<PoolSpaceBean>> model) {
            super(id, model);
        }

        /*
         * This needs to be overridden to get the most recent data from Ajax
         * autorefresh. The sequence of calls is such that the behavior's
         * beforeRender is actually called after the form's, so the BasePage
         * refresh() adapter method cannot be used in this case.
         */
        @Override
        protected void onBeforeRender() {
            /*
             * may be null if this panel is embedded
             * in a page other than PoolList
             */
            if (_poolList != null) {
                setList(_poolList.getListViewList());
            }
            super.onBeforeRender();
        }

        @Override
        protected void populateItem(final ListItem<PoolSpaceBean> item) {
            PoolSpaceBean poolBean = item.getModelObject();
            WebMarkupContainer checkboxColumn = new WebMarkupContainer("PoolPanel.checkboxRow");
            checkboxColumn.setVisibilityAllowed(_showCheckbox);
            MetaDataRoleAuthorizationStrategy.authorize(checkboxColumn,
                    RENDER, Role.ADMIN);
            CheckBox checkbox = new CheckBox("PoolPanel.selected",
                    new PropertyModel<Boolean>(poolBean, "selected"));
            checkboxColumn.add(checkbox);
            item.add(checkboxColumn);
            String stateQualifier = "";
            if (poolBean.isStatePending()) {
                item.add(AttributeModifier.replace("style", "color: #880000;"));
                stateQualifier = " (pending)";
            }
            item.add(new Label("PoolPanel.name", poolBean.getName()));
            item.add(new Label("PoolPanel.domainName",
                    poolBean.getDomainName()));
            item.add(new Label("PoolPanel.poolMode",
                    poolBean.getPoolMode().toString() + stateQualifier));
            item.add(new Label("PoolPanel.totalSpace",
                    Long.valueOf(poolBean.getTotalSpace()).toString()));
            item.add(new Label("PoolPanel.freeSpace",
                    Long.valueOf(poolBean.getFreeSpace()).toString()));
            item.add(new Label("PoolPanel.preciousSpace",
                    Long.valueOf(poolBean.getPreciousSpace()).toString()));
            item.add(new LayoutItemPanel("PoolPanel.layoutItemPanel",
                    poolBean.getPercentagePrecious(),
                    poolBean.getPercentagePinned(),
                    poolBean.getPercentageRemovable(),
                    poolBean.getPercentageFree()));
        }
    }

    public void setPoolListPage(PoolList poolList) {
        _poolList = poolList;
    }
}
