package org.dcache.webadmin.view.panels.poollist;

import java.util.List;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.panels.layout.LayoutItemPanel;
import org.dcache.webadmin.view.panels.layout.LayoutHeaderPanel;

/**
 * Reusable Panel for the Listview on Poolspaces
 * @author jans
 */
public class PoolListPanel extends BasePanel {

    private boolean _showCheckbox;

    public PoolListPanel(String id, IModel<? extends List<PoolSpaceBean>> model,
            boolean showCheckbox) {
        super(id);
        _showCheckbox = showCheckbox;
        Label selectBoxHeaderLabel = new Label("selectBoxHeader",
                getStringResource("PoolPanel.selected.header"));
        selectBoxHeaderLabel.setVisibilityAllowed(_showCheckbox);
        add(selectBoxHeaderLabel);
        add(new LayoutHeaderPanel("PoolPanel.layoutHeaderPanel"));
        add(new PoolBeanListView("poolPanelListview", model));
    }

    private class PoolBeanListView extends EvenOddListView<PoolSpaceBean> {

        PoolBeanListView(String id, IModel<? extends List<PoolSpaceBean>> model) {
            super(id, model);
        }

        @Override
        protected void populateItem(final ListItem<PoolSpaceBean> item) {
            PoolSpaceBean poolBean = item.getModelObject();
            WebMarkupContainer checkboxRow = new WebMarkupContainer("PoolPanel.checkboxRow");
            checkboxRow.setVisibilityAllowed(_showCheckbox);
            CheckBox checkbox = new CheckBox("PoolPanel.selected",
                    new PropertyModel<Boolean>(poolBean, "selected"));
            checkboxRow.add(checkbox);
            item.add(checkboxRow);
            item.add(new Label("PoolPanel.name", poolBean.getName()));
            item.add(new Label("PoolPanel.domainName",
                    poolBean.getDomainName()));
            item.add(new Label("PoolPanel.enabled",
                    Boolean.valueOf(poolBean.isEnabled()).toString()));
            item.add(new Label("PoolPanel.totalSpace",
                    Long.valueOf(poolBean.getTotalSpace()).toString()));
            item.add(new Label("PoolPanel.freeSpace",
                    Long.valueOf(poolBean.getFreeSpace()).toString()));
            item.add(new Label("PoolPanel.preciousSpace",
                    Long.valueOf(poolBean.getPreciousSpace()).toString()));
            item.add(new LayoutItemPanel("PoolPanel.layoutItemPanel",
                    poolBean.getPercentagePrecious(), poolBean.getPercentageUsed(),
                    poolBean.getPercentageFree()));
        }
    }
}
