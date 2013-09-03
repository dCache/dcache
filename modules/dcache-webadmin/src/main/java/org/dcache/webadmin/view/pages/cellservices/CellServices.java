package org.dcache.webadmin.view.pages.cellservices;

import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.head.OnLoadHeaderItem;
import org.apache.wicket.markup.head.StringHeaderItem;

import org.dcache.webadmin.controller.CellsService;
import org.dcache.webadmin.controller.exceptions.CellsServiceException;
import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.cellservices.CellServicesPanel;

/**
 * The CellServices Webpage
 * @author jans
 */
public class CellServices extends BasePage {

    private static final long serialVersionUID = -1276186550495981006L;
    private static final Logger _log = LoggerFactory.getLogger(CellServices.class);

    public CellServices() {
        add(new FeedbackPanel("feedback"));
        CellServicesPanel cellServicesPanel = new CellServicesPanel("cellServicesPanel",
                new PropertyModel(this, "cellBeans"));
        add(cellServicesPanel);
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

    @Override
    public void renderHead(IHeaderResponse response) {
        super.renderHead(response);

        response.render(new StringHeaderItem("<!-- wicket " + this.getClass().getSimpleName() + " header BEGIN -->\n"));
        response.render(JavaScriptHeaderItem.forUrl("js/picnet.table.filter.full.js"));
        response.render(JavaScriptHeaderItem.forUrl("js/jquery.tablesorter.min.js"));
        response.render(OnLoadHeaderItem.forScript(
                "                $('#sortable').tablesorter();\n"
                + "                // Initialise Plugin\n"
                + "                var options1 = {\n"
                + "                    additionalFilterTriggers: [$('#quickfind')],\n"
                + "                    clearFiltersControls: [$('#cleanfilters')],\n"
                + "                };\n"
                + "                $('#sortable').tableFilter(options1);\n"));

        response.render(new StringHeaderItem("<!-- wicket " + this.getClass().getSimpleName() + " header END -->\n"));
    }
}
