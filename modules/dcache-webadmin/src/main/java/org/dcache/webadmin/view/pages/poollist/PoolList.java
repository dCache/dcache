package org.dcache.webadmin.view.pages.poollist;

import org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.ChoiceRenderer;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.model.StringResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import diskCacheV111.pools.PoolV2Mode;
import org.dcache.webadmin.controller.PoolSpaceService;
import org.dcache.webadmin.controller.exceptions.PoolSpaceServiceException;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.beans.SelectOption;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.pages.basepage.SortableBasePage;
import org.dcache.webadmin.view.panels.poollist.PoolListPanel;
import org.dcache.webadmin.view.panels.selectall.SelectAllPanel;
import org.dcache.webadmin.view.util.Role;

/**
 * The PoolUsage Webpage
 * @author jans
 */
public class PoolList extends SortableBasePage {
    private static final int DEFAULT_DROP_DOWN_CHOICE = 0;
    private static final long serialVersionUID = -3519762401458479856L;
    private List<PoolSpaceBean> _poolBeans;
    private SelectOption _selectedOption;
    private static final Logger _log = LoggerFactory.getLogger(PoolList.class);

    /*
     * necessary so that submit uses the current list instance
     */
    private boolean submitFormCalled = false;

    public PoolList() {
        Form poolUsageForm = new PoolUsageForm("poolUsageForm");
        poolUsageForm.add(createPoolModeDropDown("mode"));
        poolUsageForm.add(new FeedbackPanel("feedback"));
        fetchPoolSpaceBeans();
        PoolListPanel poolListPanel = new PoolListPanel("poolListPanel",
                new PropertyModel(this, "_poolBeans"), true);
        poolListPanel.setPoolListPage(this);
        poolUsageForm.add(poolListPanel);
        add(poolUsageForm);
    }

    /**
     * See the {@link PoolListPanel} for why this must be used instead of
     * {@link BasePage#refresh()} here.
     */
    public List<PoolSpaceBean> getListViewList() {
        if (!submitFormCalled) {
            fetchPoolSpaceBeans();
        }
        submitFormCalled = false;
        return _poolBeans;
    }

    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        addFilterSelectScript("space", response);
    }

    private DropDownChoice<SelectOption> createPoolModeDropDown(String id) {
        List<SelectOption> dropDownChoices = getDropDownModes();
        DropDownChoice<SelectOption> dropDownChoice = new DropDownChoice<>(id,
                new PropertyModel<SelectOption>(this, "_selectedOption"),
                dropDownChoices, new ChoiceRenderer<SelectOption>("value"));
        setDefaultChoice(dropDownChoices);
        MetaDataRoleAuthorizationStrategy.authorize(dropDownChoice,
                RENDER, Role.ADMIN);
        return dropDownChoice;
    }

    private void setDefaultChoice(List<SelectOption> dropDownChoices) {
        _selectedOption = dropDownChoices.get(DEFAULT_DROP_DOWN_CHOICE);
    }

    private List<SelectOption> getDropDownModes() {
        List<SelectOption> list = new ArrayList<>(3);
        list.add(new SelectOption(PoolV2Mode.ENABLED, new StringResourceModel(
                "mode.ENABLED", this, null).getString()));
        list.add(new SelectOption(PoolV2Mode.DISABLED_STRICT, new StringResourceModel(
                "mode.DISABLED_STRICT", this, null).getString()));
        list.add(new SelectOption(PoolV2Mode.DISABLED_RDONLY, new StringResourceModel(
                "mode.DISABLED_RDONLY", this, null).getString()));
        return list;
    }

    private PoolSpaceService getPoolSpaceService() {
        return getWebadminApplication().getPoolSpaceService();
    }

    private void fetchPoolSpaceBeans() {
        try {
            _log.debug("getPoolListAction called");
            _poolBeans = getPoolSpaceService().getPoolBeans();
        } catch (PoolSpaceServiceException ex) {
            this.error(getStringResource("error.getPoolsFailed") + ex.getMessage());
            _log.debug("getPoolListAction failed {}", ex.getMessage());
            _poolBeans = null;
        }
    }

    private class PoolUsageForm extends Form {

        private static final long serialVersionUID = -6416804759275113231L;

        public PoolUsageForm(String id) {
            super(id);
            Button submit = new Button("submit");
            SelectAllPanel selectAllPanel = new SelectAllPanel("selectAllPanel", submit) {
                private static final long serialVersionUID = -1886067539481596863L;

                @Override
                protected void setSubmitCalled() {
                    submitFormCalled = true;
                }

                @Override
                protected void setSelectionForAll(Boolean selected) {
                    for (PoolSpaceBean bean : _poolBeans) {
                        bean.setSelected(selected);
                    }
                }
            };
            this.add(selectAllPanel);
        }

        @Override
        protected void onSubmit() {
            submitFormCalled = true;
            _log.debug("button pressed");
            if (_poolBeans != null && _selectedOption != null) {
                try {
                    _log.debug("selected: {}", _selectedOption.getValue());
                    PoolV2Mode poolMode = new PoolV2Mode(_selectedOption.getKey());
                    getPoolSpaceService().changePoolMode(_poolBeans, poolMode,
                            getWebadminSession().getUserName());
                } catch (PoolSpaceServiceException ex) {
                    _log.error("something went wrong with enable/disable");
                    this.error(getStringResource("error.changePoolModeFailed") + ex.getMessage());
                }
            }
        }
    }
}
