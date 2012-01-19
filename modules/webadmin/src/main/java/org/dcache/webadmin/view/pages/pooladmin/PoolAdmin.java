package org.dcache.webadmin.view.pages.pooladmin;

import java.util.ArrayList;
import java.util.List;
import org.apache.wicket.behavior.SimpleAttributeModifier;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.basic.MultiLineLabel;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.controller.PoolAdminService;
import org.dcache.webadmin.controller.exceptions.PoolAdminServiceException;
import org.dcache.webadmin.view.beans.PoolAdminBean;
import org.dcache.webadmin.view.beans.PoolCommandBean;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.util.DefaultFocusBehaviour;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.dcache.webadmin.view.util.SelectableWrapper;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * The PoolAdmin Webpage
 * @author jans
 */
public class PoolAdmin extends BasePage implements AuthenticatedWebPage {

    private static final String EMPTY_STRING = "";
    public static final int RESPONSE_CUTOFF_INDEX_MULTIPLE_POOLS = 120;
    private static final Logger _log = LoggerFactory.getLogger(PoolAdmin.class);
    private List<PoolAdminBean> _poolGroups = new ArrayList<PoolAdminBean>();
    private PoolAdminBean _currentPoolGroup;
    private String _command = "";
    private String _lastCommand = "";

    public PoolAdmin() {
        getPoolGroupsAction();
        addMarkup();
    }

    private void addMarkup() {
        Form poolAdminForm = new Form("poolAdminForm");
        poolAdminForm.add(new FeedbackPanel("feedback"));
        TextField commandInput = new TextField("commandText",
                new PropertyModel(this, "_command"));
        commandInput.setRequired(true);
        poolAdminForm.add(new SubmitButton("submit"));
        commandInput.add(new DefaultFocusBehaviour());
        poolAdminForm.add(commandInput);
        poolAdminForm.add(new Label("lastCommand",
                new PropertyModel(this, "_lastCommand")));
        ListView<PoolAdminBean> poolGroups =
                buildPoolGroupView("poolGroupList");
        poolAdminForm.add(new SelectAllButton("selectAllButton"));
        poolAdminForm.add(new DeselectAllButton("deselectAllButton"));
        poolAdminForm.add(poolGroups);
        ListView<SelectableWrapper<PoolCommandBean>> poolItems =
                buildPoolItemView("poolItems");
        poolAdminForm.add(poolItems);
        add(poolAdminForm);
    }

    private void clearResponses() {
        if (_currentPoolGroup != null) {
            for (SelectableWrapper<PoolCommandBean> pool : _currentPoolGroup.getPools()) {
                pool.getWrapped().setResponse(EMPTY_STRING);
            }
        }
    }

    private void deselectAll() {
        setSelectionForAll(Boolean.FALSE);
    }

    private void selectAll() {
        setSelectionForAll(Boolean.TRUE);
    }

    private void setSelectionForAll(Boolean selected) {
        if (_currentPoolGroup != null) {
            for (SelectableWrapper wrapper : _currentPoolGroup.getPools()) {
                wrapper.setSelected(selected);
            }
        }
    }

    private boolean isAtLeastOneSelected() {
        boolean oneIsSelected = false;
        if (_currentPoolGroup != null) {
            for (SelectableWrapper wrapper : _currentPoolGroup.getPools()) {
                if (wrapper.isSelected()) {
                    oneIsSelected = true;
                    break;
                }
            }
        }
        return oneIsSelected;
    }

    private void getPoolGroupsAction() {
        try {
            _poolGroups = getPoolAdminService().getPoolGroups();
        } catch (PoolAdminServiceException e) {
            error(getStringResource("error.noPoolGroups"));
            _log.error("could not retrieve Pool Groups: {}", e.getMessage());
        }
    }

    private PoolAdminService getPoolAdminService() {
        return getWebadminApplication().getPoolAdminService();
    }

    private ListView<PoolAdminBean> buildPoolGroupView(String id) {
        return new ListView<PoolAdminBean>(
                id, new PropertyModel<List<PoolAdminBean>>(
                this, "_poolGroups")) {

            @Override
            protected void populateItem(ListItem item) {
                PoolAdminBean poolGroup = (PoolAdminBean) item.getModelObject();
                item.add(createLink(poolGroup));
                handleActiveLink(item, poolGroup);
            }

            private Link createLink(final PoolAdminBean poolGroup) {
                Link link = new Link("poolGroupLink") {

                    @Override
                    public void onClick() {
                        if (!poolGroup.equals(_currentPoolGroup)) {
                            clearResponses();
                            deselectAll();
                            _lastCommand = EMPTY_STRING;
                            _currentPoolGroup = poolGroup;
                        }
                    }
                };
                link.add(new Label("poolGroupLinkMessage",
                        poolGroup.getGroupName()));
                return link;
            }
        };
    }

    private void handleActiveLink(ListItem link, PoolAdminBean poolGroup) {
        if (isActiveLink(poolGroup)) {
            addActiveAttribute(link);
        }
    }

    private boolean isActiveLink(PoolAdminBean poolGroup) {
        return poolGroup.equals(_currentPoolGroup);
    }

    private void addActiveAttribute(ListItem item) {
        item.add(new SimpleAttributeModifier("class", "active"));
    }

    private ListView<SelectableWrapper<PoolCommandBean>> buildPoolItemView(String id) {
        return new EvenOddListView<SelectableWrapper<PoolCommandBean>>(
                id, new PropertyModel<List<SelectableWrapper<PoolCommandBean>>>(
                this, "_currentPoolGroup._pools")) {

            @Override
            protected void populateItem(ListItem item) {
                SelectableWrapper<PoolCommandBean> pool =
                        (SelectableWrapper<PoolCommandBean>) item.getModelObject();
                item.add(new CheckBox("poolAdmin.poolselected",
                        new PropertyModel<Boolean>(pool, "_selected")));
                item.add(new Label("poolAdmin.poolnamevalue",
                        pool.getWrapped().getName()));
                item.add(new Label("poolAdmin.pooldomainvalue",
                        pool.getWrapped().getDomain()));
                item.add(new MultiLineLabel("poolAdmin.poolresponsevalue",
                        pool.getWrapped().getResponse()));
            }
        };
    }

    private void cutResponseForMultipleSelection() {
        if (areMultiplePoolsSelected()) {
            cutResponses();
        }
    }

    private boolean areMultiplePoolsSelected() {
        int selectedCount = 0;
        for (SelectableWrapper wrapper : _currentPoolGroup.getPools()) {
            if (wrapper.isSelected()) {
                selectedCount++;
            }
        }
        return (selectedCount > 1);
    }

    private void cutResponses() {
        for (SelectableWrapper<PoolCommandBean> wrapper : _currentPoolGroup.getPools()) {
            if (wrapper.isSelected()) {
                String response = wrapper.getWrapped().getResponse();
                if (isLongerThanCutoff(response)) {
                    _log.debug("response longer than cutoff");
                    String cutResponse = response.substring(
                            0, RESPONSE_CUTOFF_INDEX_MULTIPLE_POOLS - 1);
                    wrapper.getWrapped().setResponse(cutResponse
                            + getStringResource("poolAdmin.cutoffMessage"));
                }
            }
        }
    }

    private boolean isLongerThanCutoff(String response) {
        return (response.length() > RESPONSE_CUTOFF_INDEX_MULTIPLE_POOLS);
    }

    private class SubmitButton extends Button {

        public SubmitButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            if (_currentPoolGroup != null) {
                try {
                    _log.debug("button pressed with group {} and command {}",
                            _currentPoolGroup.getGroupName(), _command);
                    _lastCommand = _command;
                    if (isAtLeastOneSelected()) {
                        getPoolAdminService().sendCommand(_currentPoolGroup.getPools(),
                                _command);
                        cutResponseForMultipleSelection();
                        deselectAll();
                    } else {
                        error(getStringResource("error.noSelection"));
                    }
                } catch (PoolAdminServiceException e) {
                    _log.error("couldn't send all PoolCommands successful, {}",
                            e.getMessage());
                    error("couldn't send all PoolCommands successful " + e.getMessage());
                }
            }
        }
    }

    private class SelectAllButton extends Button {

        public SelectAllButton(String id) {
            super(id);
            this.setDefaultFormProcessing(false);
        }

        @Override
        public void onSubmit() {
            selectAll();
        }
    }

    private class DeselectAllButton extends Button {

        public DeselectAllButton(String id) {
            super(id);
            this.setDefaultFormProcessing(false);
        }

        @Override
        public void onSubmit() {
            deselectAll();
        }
    }
}
