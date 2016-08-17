package org.dcache.webadmin.view.pages.celladmin;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.form.AjaxFormComponentUpdatingBehavior;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.basic.MultiLineLabel;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.AbstractReadOnlyModel;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.protocol.https.RequireHttps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.exceptions.CellAdminServiceException;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.util.DefaultFocusBehaviour;

/**
 * The cellAdmin Webpage
 * @author jans
 */
@RequireHttps
public class CellAdmin extends BasePage implements AuthenticatedWebPage {

    private static final String EMPTY_STRING = "";
    private static final Logger _log = LoggerFactory.getLogger(CellAdmin.class);
    private static final long serialVersionUID = -61395248592530110L;
    private String _selectedDomain;
    private String _selectedCell;
    private String _command = "";
    private String _lastCommand = "";
    private String _response = "";

    public CellAdmin() {
        addMarkup();
    }

    private  Map<String, List<String>> getDomainMap() {
        try {
            return getCellAdminService().getDomainMap();
        } catch (CellAdminServiceException e) {
            error(getStringResource("error.noCells"));
            _log.error("could not retrieve cells: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    private void addMarkup() {
        Form cellAdminForm = new Form("cellAdminForm");
        final DropDownChoice<String> domains = new DropDownChoice<>("cellAdminDomain",
                new PropertyModel<String>(this, "_selectedDomain"),
                new DomainsModel());
        cellAdminForm.add(domains);
        final DropDownChoice<String> cells = new DropDownChoice<>("cellAdminCell",
                new PropertyModel<String>(this, "_selectedCell"),
                new CellsModel());
        cells.setRequired(true);
        cells.setOutputMarkupId(true);
        cellAdminForm.add(cells);
        domains.add(new AjaxFormComponentUpdatingBehavior("change") {

            private static final long serialVersionUID = 7202016450667815788L;

            @Override
            protected void onUpdate(AjaxRequestTarget target) {
                if (target != null) {
                    target.add(cells);
                } else {
//                implement fallback for non javascript clients
                    cells.updateModel();
                }
            }
        });
        cellAdminForm.add(new FeedbackPanel("feedback"));
        TextField commandInput = new TextField("commandText",
                new PropertyModel(this, "_command"));
        commandInput.add(new DefaultFocusBehaviour());
        commandInput.setRequired(true);
        cellAdminForm.add(commandInput);
        cellAdminForm.add(new SubmitButton("submit"));
        cellAdminForm.add(new Label("lastCommand",
                new PropertyModel(this, "_lastCommand")) {

            private static final long serialVersionUID = 4773251450645556487L;

            @Override
            protected void onConfigure() {
                setVisibilityAllowed(!_lastCommand.isEmpty());
            }
        });
        cellAdminForm.add(new Label("cellAdmin.receiver",
                new ReceiverModel()));
        cellAdminForm.add(new MultiLineLabel("cellAdmin.cellresponsevalue",
                new PropertyModel(this, "_response")) {

            private static final long serialVersionUID = 4018965991481863398L;

            @Override
            protected void onConfigure() {
                setVisibilityAllowed(!_response.isEmpty());
            }
        });
        add(cellAdminForm);
    }

    private void clearResponse() {
        _response = EMPTY_STRING;
    }

    private CellAdminService getCellAdminService() {
        return getWebadminApplication().getCellAdminService();
    }

    private class SubmitButton extends Button {

        private static final long serialVersionUID = 8440840991844087035L;

        public SubmitButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            try {
                String target = _selectedCell + "@"
                        + _selectedDomain;
                _log.debug("submit pressed with cell {} and command {}",
                        target, _command);
                _lastCommand = getStringResource("cellAdmin.lastCommand") +" "+ _command;
                clearResponse();
                _response = getCellAdminService().sendCommand(target, _command);
            } catch (CellAdminServiceException e) {
                _log.error("couldn't send CellCommand, {}",
                        e.getMessage());
                error(getStringResource("error.failure"));
            }
        }
    }

    private class DomainsModel extends AbstractReadOnlyModel<List<String>> {

        private static final long serialVersionUID = 1232126026333463479L;

        @Override
        public List<String> getObject() {
            List<String> domains = new ArrayList<>(getDomainMap().keySet());
            Collections.sort(domains);
            return domains;
        }
    }

    private class CellsModel extends AbstractReadOnlyModel<List<String>> {

        private static final long serialVersionUID = -5346050077644898205L;

        @Override
        public List<String> getObject() {
            List<String> cells = getDomainMap().get(_selectedDomain);
            if (cells == null) {
                cells = Collections.emptyList();
            }
            Collections.sort(cells);
            return cells;
        }
    }

    private class ReceiverModel extends AbstractReadOnlyModel<String> {

        private static final long serialVersionUID = 1297395223042861665L;

        @Override
        public String getObject() {
            if (_selectedCell != null && _selectedDomain != null) {
                return getStringResource("header2") + _selectedCell + "@" + _selectedDomain;
            }
            return "";
        }
    }
}
