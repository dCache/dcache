package org.dcache.webadmin.view.panels.activetransfers;

import java.util.List;
import org.apache.wicket.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.Role;
import org.dcache.webadmin.view.util.SelectableWrapper;

/**
 * Reusable Panel for the Listview on Active Transfers
 * @author jans
 */
public class ActiveTransfersPanel extends BasePanel {

    public ActiveTransfersPanel(String id,
            IModel<? extends List<SelectableWrapper<ActiveTransfersBean>>> model) {
        super(id);
        Label selectBoxHeaderLabel = new Label("selectBoxHeader",
                getStringResource("activeTransfersPanel.selected.header"));
        MetaDataRoleAuthorizationStrategy.authorize(selectBoxHeaderLabel,
                RENDER, Role.ADMIN);
        add(selectBoxHeaderLabel);
        add(new ActiveTransfersBeanListView("activeTransfersPanelListview", model));
    }

    private class ActiveTransfersBeanListView extends EvenOddListView<SelectableWrapper<ActiveTransfersBean>> {

        ActiveTransfersBeanListView(String id,
                IModel<? extends List<SelectableWrapper<ActiveTransfersBean>>> model) {
            super(id, model);
        }

        @Override
        protected void populateItem(
                final ListItem<SelectableWrapper<ActiveTransfersBean>> item) {
            SelectableWrapper<ActiveTransfersBean> wrapper = item.getModelObject();
            ActiveTransfersBean activeTransfer = wrapper.getWrapped();
            WebMarkupContainer checkboxColumn =
                    new WebMarkupContainer("activeTransfersPanel.checkboxRow");
            MetaDataRoleAuthorizationStrategy.authorize(checkboxColumn,
                    RENDER, Role.ADMIN);
            CheckBox checkbox = new CheckBox("activeTransfersPanel.selected",
                    new PropertyModel<Boolean>(wrapper, "selected"));
            checkboxColumn.add(checkbox);
            item.add(checkboxColumn);
            Label doorLabel = new Label("activeTransfersPanel.door", activeTransfer.getCellName());
            MetaDataRoleAuthorizationStrategy.authorize(doorLabel, RENDER, Role.ADMIN);
            item.add(doorLabel);
            item.add(new Label("activeTransfersPanel.domain", activeTransfer.getCellDomainName()));
            item.add(new Label("activeTransfersPanel.seq", activeTransfer.getSerialIdString()));
            String protocol = activeTransfer.getProtocolFamily() + "-" +
                    activeTransfer.getProtocolVersion();
            item.add(new Label("activeTransfersPanel.protocol", protocol));
            item.add(new Label("activeTransfersPanel.owner", activeTransfer.getOwner()));
            item.add(new Label("activeTransfersPanel.process", activeTransfer.getProcess()));
            item.add(new Label("activeTransfersPanel.pnfsid", activeTransfer.getPnfsId()));
            item.add(new Label("activeTransfersPanel.pool", activeTransfer.getPool()));
            Label hostLabel = new Label("activeTransfersPanel.host", activeTransfer.getReplyHost());
            MetaDataRoleAuthorizationStrategy.authorize(hostLabel, RENDER, Role.ADMIN);
            item.add(hostLabel);
            item.add(new Label("activeTransfersPanel.status", activeTransfer.getStatus()));
            item.add(new Label("activeTransfersPanel.since", activeTransfer.getWaitingSinceTime()));
            item.add(new Label("activeTransfersPanel.health", activeTransfer.getState()));
            item.add(new Label("activeTransfersPanel.transferred",
                    Long.valueOf(activeTransfer.getTransferred()).toString()));
            item.add(new Label("activeTransfersPanel.speed", activeTransfer.getTransferRate()));
            item.add(new Label("activeTransfersPanel.jobid",
                    Long.valueOf(activeTransfer.getJobId()).toString()));

        }
    }
}
