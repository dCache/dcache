package org.dcache.webadmin.view.panels.cellservices;

import java.util.List;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.IModel;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;

/**
 * Reusable Panel for the Listview on CellServices
 * @author jans
 */
public class CellServicesPanel extends BasePanel {

    public CellServicesPanel(String id,
            IModel<? extends List<CellServicesBean>> model) {
        super(id);
        ListView<CellServicesBean> listview =
                new EvenOddListView<CellServicesBean>("CellServicesPanelListview",
                model) {

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
