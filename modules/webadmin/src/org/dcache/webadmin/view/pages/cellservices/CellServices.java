package org.dcache.webadmin.view.pages.cellservices;

import java.util.List;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.exceptions.CellsServiceException;
import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.cellservices.CellServicesPanel;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * The CellServices Webpage
 * @author jans
 */
public class CellServices extends BasePage implements AuthenticatedWebPage {

    private List<CellServicesBean> _cellBeans;
    private static final Logger _log = LoggerFactory.getLogger(CellServices.class);

    public CellServices() {
        add(new FeedbackPanel("feedback"));
        getCellServicesAction();
        CellServicesPanel cellServicesPanel = new CellServicesPanel("cellServicesPanel",
                new PropertyModel(this, "_cellBeans"));
        add(cellServicesPanel);
    }

    private CellsService getCellsService() {
        return getWebadminApplication().getCellsService();
    }

    private void getCellServicesAction() {
        try {
            _log.debug("getCellServicesAction called");
            _cellBeans = getCellsService().getCellServicesBeans();
        } catch (CellsServiceException ex) {
            this.error(getStringResource("error.getCellsFailed") + ex.getMessage());
            _log.debug("getCellServicesAction failed {}", ex.getMessage());
            _cellBeans = null;
        }
    }
}
