package org.dcache.webadmin.view.pages.activetransfers;

import org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.head.OnLoadHeaderItem;
import org.apache.wicket.markup.head.StringHeaderItem;

import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.exceptions.ActiveTransfersServiceException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.activetransfers.ActiveTransfersPanel;
import org.dcache.webadmin.view.util.Role;
import org.dcache.webadmin.view.util.SelectableWrapper;

public class ActiveTransfers extends BasePage {

    private static final Logger _log = LoggerFactory.getLogger(ActiveTransfers.class);
    private static final long serialVersionUID = -1360523434922193867L;
    private List<SelectableWrapper<ActiveTransfersBean>> _activeTransfers;

    public ActiveTransfers() {
        Form activeTransfersForm = new Form("activeTransfersForm");
        activeTransfersForm.add(new FeedbackPanel("feedback"));
        Button button = new SubmitButton("submit");
        MetaDataRoleAuthorizationStrategy.authorize(button, RENDER, Role.ADMIN);
        activeTransfersForm.add(button);
        getActiveTransfers();
        activeTransfersForm.add(new ActiveTransfersPanel("activeTransfersPanel",
                new PropertyModel(this, "_activeTransfers")));
        add(activeTransfersForm);
    }

    private ActiveTransfersService getActiveTransfersService() {
        return getWebadminApplication().getActiveTransfersService();
    }

    private void getActiveTransfers() {
        try {
            _log.debug("getActiveTransfers called");
            _activeTransfers = getActiveTransfersService().getActiveTransferBeans();
        } catch (ActiveTransfersServiceException ex) {
            this.error(getStringResource("error.getActiveTransfersFailed") + ex.getMessage());
            _log.debug("getActiveTransfers failed {}", ex.getMessage());
            _activeTransfers = null;
        }
    }

    private class SubmitButton extends Button {

        private static final long serialVersionUID = -1564058161768591840L;

        public SubmitButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            try {
                _log.debug("Kill Movers submitted");
                getActiveTransfersService().killTransfers(_activeTransfers);
            } catch (ActiveTransfersServiceException e) {
                _log.info("couldn't kill some movers - jobIds: {}",
                        e.getMessage());
                error(getStringResource("error.notAllMoversKilled"));
            }
            getActiveTransfers();
        }
    }

    @Override
    public void renderHead(IHeaderResponse response) {
        super.renderHead(response);

        response.render(new StringHeaderItem("<!-- wicket " + this.getClass().getSimpleName() + " header BEGIN -->\n"));;
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
