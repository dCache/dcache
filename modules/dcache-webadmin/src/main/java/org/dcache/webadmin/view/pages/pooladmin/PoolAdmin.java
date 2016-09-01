package org.dcache.webadmin.view.pages.pooladmin;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.head.IHeaderResponse;
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
import org.apache.wicket.protocol.https.RequireHttps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import org.dcache.webadmin.controller.PoolAdminService;
import org.dcache.webadmin.controller.exceptions.PoolAdminServiceException;
import org.dcache.webadmin.view.beans.PoolAdminBean;
import org.dcache.webadmin.view.beans.PoolCommandBean;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.SortableBasePage;
import org.dcache.webadmin.view.panels.selectall.SelectAllPanel;
import org.dcache.webadmin.view.util.DefaultFocusBehaviour;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.dcache.webadmin.view.util.SelectableWrapper;

/**
 * The PoolAdmin Webpage
 * @author jans
 */
@RequireHttps
public class PoolAdmin extends SortableBasePage implements AuthenticatedWebPage {

    private static final String EMPTY_STRING = "";
    public static final int RESPONSE_CUTOFF_INDEX_MULTIPLE_POOLS = 120;
    private static final Logger _log = LoggerFactory.getLogger(PoolAdmin.class);
    private static final long serialVersionUID = -3790266074783564167L;

    private class PoolAdminTableSelectAll extends SelectAllPanel {
        private static final long serialVersionUID = -1886067539481596863L;

        public PoolAdminTableSelectAll(String id, Button submit) {
            super(id, "admin", submit);
        }

        @Override
        protected void setSubmitCalled() {
            // NOP
        }

        @Override
        protected void setSelectionForAll(Boolean selected) {
            if (_currentPoolGroup != null) {
                for (SelectableWrapper<PoolCommandBean> wrapper : _currentPoolGroup.getPools()) {
                    if (!isHidden(wrapper.getWrapped())) {
                        wrapper.setSelected(selected);
                    }
                }
            }
        }
    }

    private class SubmitButton extends Button {
        private static final long serialVersionUID = -944235821155061987L;

        public SubmitButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            if (_currentPoolGroup != null) {
                try {
                    _log.debug("button pressed with group {} and command {}",
                               _currentPoolGroup.getGroupName(), _command);
                    _lastCommand = getStringResource("poolAdmin.lastCommand") +" "+ _command;
                    if (isAtLeastOneSelected()) {
                        getPoolAdminService().sendCommand(_currentPoolGroup.getPools(),
                                                          _command);
                        cutResponseForMultipleSelection();
                        selectAll.setSelectionForAll(Boolean.FALSE);
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

    private PoolAdminBean _currentPoolGroup;
    private PoolAdminTableSelectAll selectAll;
    private String _command = "";
    private String _lastCommand = "";

    public PoolAdmin() {
        addMarkup();
    }

    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        addFilterSelectScript("admin", response);
    }

    private void addMarkup() {
        Form<?> poolAdminForm = new Form<Void>("poolAdminForm");
        poolAdminForm.add(new FeedbackPanel("feedback"));
        TextField<Object> commandInput = new TextField<>("commandText",
                new PropertyModel<>(this, "_command"));
        commandInput.setRequired(true);
        commandInput.add(new DefaultFocusBehaviour());
        poolAdminForm.add(commandInput);
        poolAdminForm.add(new Label("lastCommand",
                new PropertyModel<>(this, "_lastCommand")) {

            private static final long serialVersionUID = -2390293990674335313L;

            @Override
            protected void onConfigure() {
                setVisibilityAllowed(!_lastCommand.isEmpty());
            }
        });
        ListView<PoolAdminBean> poolGroups =
                buildPoolGroupView("poolGroupList");
        selectAll = new PoolAdminTableSelectAll("selectAllPanel",
                                                new SubmitButton("submit"));
        poolAdminForm.add(selectAll);
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

    private boolean isAtLeastOneSelected() {
        boolean oneIsSelected = false;
        if (_currentPoolGroup != null) {
            for (SelectableWrapper<PoolCommandBean> wrapper : _currentPoolGroup.getPools()) {
                if (wrapper.isSelected()) {
                    oneIsSelected = true;
                    break;
                }
            }
        }
        return oneIsSelected;
    }

    public List<PoolAdminBean> getPoolGroups() {
        try {
            return getPoolAdminService().getPoolGroups();
        } catch (PoolAdminServiceException e) {
            error(getStringResource("error.noPoolGroups"));
            _log.error("could not retrieve Pool Groups: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private PoolAdminService getPoolAdminService() {
        return getWebadminApplication().getPoolAdminService();
    }

    private ListView<PoolAdminBean> buildPoolGroupView(String id) {
        return new ListView<PoolAdminBean>(
                id, new PropertyModel<List<PoolAdminBean>>(
                this, "poolGroups")) {

            private static final long serialVersionUID = 6196065833753259467L;

            @Override
            protected void populateItem(ListItem<PoolAdminBean> item) {
                PoolAdminBean poolGroup = item.getModelObject();
                item.add(createLink(poolGroup));
                handleActiveLink(item, poolGroup);
            }

            private Link<Object> createLink(final PoolAdminBean poolGroup) {
                Link<Object> link = new Link<Object>("poolGroupLink") {

                    private static final long serialVersionUID = -3526116608918348941L;

                    @Override
                    public void onClick() {
                        if (!poolGroup.equals(_currentPoolGroup)) {
                            clearResponses();
                            selectAll.setSelectionForAll(Boolean.FALSE);
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

    private void handleActiveLink(ListItem<?> link, PoolAdminBean poolGroup) {
        if (isActiveLink(poolGroup)) {
            addActiveAttribute(link);
        }
    }

    private boolean isActiveLink(PoolAdminBean poolGroup) {
        return poolGroup.equals(_currentPoolGroup);
    }

    private void addActiveAttribute(ListItem<?> item) {
        item.add(new AttributeModifier("class", "active"));
    }

    private ListView<SelectableWrapper<PoolCommandBean>> buildPoolItemView(String id) {
        return new EvenOddListView<SelectableWrapper<PoolCommandBean>>(
                id, new PropertyModel<List<SelectableWrapper<PoolCommandBean>>>(
                this, "_currentPoolGroup._pools")) {

            private static final long serialVersionUID = 4362565859710920442L;

            @Override
            protected void populateItem(ListItem<SelectableWrapper<PoolCommandBean>> item) {
                SelectableWrapper<PoolCommandBean> pool =
                        item.getModelObject();
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
        for (SelectableWrapper<PoolCommandBean> wrapper : _currentPoolGroup.getPools()) {
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
}
