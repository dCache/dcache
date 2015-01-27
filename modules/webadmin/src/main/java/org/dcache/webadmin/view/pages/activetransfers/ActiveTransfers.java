package org.dcache.webadmin.view.pages.activetransfers;

import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.exceptions.ActiveTransfersServiceException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.activetransfers.ActiveTransfersPanel;
import org.dcache.webadmin.view.panels.selectall.SelectAllPanel;
import org.dcache.webadmin.view.util.SelectableWrapper;

public class ActiveTransfers extends BasePage {

    private static final Logger _log = LoggerFactory.getLogger(ActiveTransfers.class);
    private static final long serialVersionUID = -1360523434922193867L;

    private List<SelectableWrapper<ActiveTransfersBean>> _transfers;

    /*
     * necessary so that submit uses the current list instance
     */
    private boolean submitFormCalled = false;

    public ActiveTransfers() {
        Form<?> activeTransfersForm = new Form<Void>("activeTransfersForm");
        activeTransfersForm.add(new FeedbackPanel("feedback"));
        Button submit = new SubmitButton("submit");
        SelectAllPanel selectAll = new SelectAllPanel("selectAllPanel", submit){
            private static final long serialVersionUID = -1886067539481596863L;

            @Override
            protected void setSubmitCalled() {
                submitFormCalled = true;
            }

            @Override
            protected void setSelectionForAll(Boolean selected) {
                for (SelectableWrapper<ActiveTransfersBean> wrapper: _transfers) {
                    wrapper.setSelected(selected);
                }
            }
        };
        activeTransfersForm.add(selectAll);
        fetchActiveTransfers();
        ActiveTransfersPanel panel = new ActiveTransfersPanel("activeTransfersPanel",
                        new PropertyModel(this, "_transfers"));
        panel.setActiveTransfersPage(this);
        activeTransfersForm.add(panel);
        add(activeTransfersForm);
    }

    public List<SelectableWrapper<ActiveTransfersBean>> getListViewList() {
        if (!submitFormCalled) {
            fetchActiveTransfers();
        }
        submitFormCalled = false;
        return _transfers;
    }

    private ActiveTransfersService getActiveTransfersService() {
        return getWebadminApplication().getActiveTransfersService();
    }

    public void fetchActiveTransfers() {
        try {
            _log.debug("getActiveTransfers called");
            _transfers = getActiveTransfersService().getActiveTransferBeans();
        } catch (ActiveTransfersServiceException ex) {
            this.error(getStringResource("error.getActiveTransfersFailed") + ex.getMessage());
            _log.debug("getActiveTransfers failed {}", ex.getMessage());
            _transfers = Collections.emptyList();
        }
    }

    private class SubmitButton extends Button {

        private static final long serialVersionUID = -1564058161768591840L;

        public SubmitButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            submitFormCalled = true;
            try {
                _log.debug("Kill Movers submitted");
                getActiveTransfersService().killTransfers(_transfers);
            } catch (ActiveTransfersServiceException e) {
                _log.info("couldn't kill some movers - jobIds: {}",
                        e.getMessage());
                error(getStringResource("error.notAllMoversKilled"));
            }
        }
    }
}
