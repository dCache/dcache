package org.dcache.webadmin.view.panels.cellservices;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.IModel;

import java.util.List;

import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.EvenOddListView;

/**
 * Reusable Panel for the Listview on CellServices
 * @author jans
 */
public class CellServicesPanel extends BasePanel {

    private static final long serialVersionUID = 5170058812728756588L;

    public CellServicesPanel(String id,
            IModel<? extends List<CellServicesBean>> model) {
        super(id);
        ListView<CellServicesBean> listview =
                new EvenOddListView<CellServicesBean>("CellServicesPanelListview",
                model) {

                    private static final long serialVersionUID = 4914875905306796317L;

                    @Override
                    protected void populateItem(ListItem item) {
                        CellServicesBean cell = (CellServicesBean) item.getModelObject();
                        item.add(new Label("CellServicesPanel.name", cell.getName()));
                        item.add(new Label("CellServicesPanel.domainName",
                                cell.getDomainName()));
                        item.add(new Label("CellServicesPanel.EventQueues",
                                String.valueOf(cell.getEventQueueSize())));
                        item.add(new Label("CellServicesPanel.Threadcount",
                                String.valueOf(cell.getThreadCount())));
                        item.add(new Label("CellServicesPanel.Ping", cell.getPing()));
                        item.add(new Label("CellServicesPanel.CreationTime",
                                cell.getCreatedDateTime()));
                        item.add(new Label("CellServicesPanel.Version",
                                cell.getVersion()));
                    }
                };
        add(listview);
    }
}
