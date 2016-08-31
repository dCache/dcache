package org.dcache.webadmin.view.pages.cellservices;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.exceptions.CellsServiceException;
import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.pages.basepage.SortableBasePage;
import org.dcache.webadmin.view.panels.cellservices.CellServicesPanel;

/**
 * The CellServices Webpage
 * @author jans
 */
public class CellServices extends SortableBasePage {

    private static final long serialVersionUID = -1276186550495981006L;
    private static final Logger _log = LoggerFactory.getLogger(CellServices.class);

    public CellServices() {
        Form<?> form = new Form<Void>("cellServicesForm");
        form.add(new FeedbackPanel("feedback"));
        CellServicesPanel cellServicesPanel = new CellServicesPanel("cellServicesPanel",
                new PropertyModel(this, "cellBeans"));
        form.add(cellServicesPanel);
        add(form);
    }

    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        addFilterSelectScript("cells", response);
    }

    private CellsService getCellsService() {
        return getWebadminApplication().getCellsService();
    }

    public List<CellServicesBean> getCellBeans()
    {
        try {
            _log.debug("getCellServicesAction called");
            return getCellsService().getCellServicesBeans();
        } catch (CellsServiceException ex) {
            this.error(getStringResource("error.getCellsFailed") + ex.getMessage());
            _log.debug("getCellServicesAction failed {}", ex.getMessage());
            return null;
        }
    }
}
