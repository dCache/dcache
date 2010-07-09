package org.dcache.webadmin.view.pages.poollist;

import diskCacheV111.pools.PoolV2Mode;
import java.util.List;
import java.util.ArrayList;
import org.apache.wicket.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.ChoiceRenderer;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.model.StringResourceModel;
import org.dcache.webadmin.controller.PoolBeanService;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.view.beans.PoolBean;
import org.dcache.webadmin.view.beans.SelectOption;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.poollist.PoolListPanel;
import org.dcache.webadmin.view.util.Role;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * The PoolUsage Webpage
 * @author jans
 */
public class PoolList extends BasePage implements AuthenticatedWebPage {

    private static final int DEFAULT_DROP_DOWN_CHOICE = 0;
    private List<PoolBean> _poolBeans;
    private SelectOption _selectedOption;
    private static final Logger _log = LoggerFactory.getLogger(PoolList.class);

    public PoolList() {
        Form poolUsageForm = new PoolUsageForm("poolUsageForm");
        poolUsageForm.add(createPoolModeDropDown("mode"));
        poolUsageForm.add(new FeedbackPanel("feedback"));
        getPoolsAction();
        PoolListPanel poolListPanel = new PoolListPanel("poolListPanel",
                new PropertyModel(this, "_poolBeans"), true);
        poolUsageForm.add(poolListPanel);
        add(poolUsageForm);
    }

    private DropDownChoice createPoolModeDropDown(String id) {
        List<SelectOption> dropDownChoices = getDropDownModes();
        DropDownChoice dropDownChoice = new DropDownChoice(id,
                new PropertyModel(this, "_selectedOption"),
                dropDownChoices, new ChoiceRenderer<SelectOption>("value"));
        setDefaultChoice(dropDownChoices);
        return dropDownChoice;
    }

    private void setDefaultChoice(List<SelectOption> dropDownChoices) {
        _selectedOption = dropDownChoices.get(DEFAULT_DROP_DOWN_CHOICE);
    }

    private List<SelectOption> getDropDownModes() {
        List<SelectOption> list = new ArrayList<SelectOption>(3);
        list.add(new SelectOption(PoolV2Mode.ENABLED, new StringResourceModel(
                "mode.ENABLED", this, null).getString()));
        list.add(new SelectOption(PoolV2Mode.DISABLED_STRICT, new StringResourceModel(
                "mode.DISABLED_STRICT", this, null).getString()));
        list.add(new SelectOption(PoolV2Mode.DISABLED_RDONLY, new StringResourceModel(
                "mode.DISABLED_RDONLY", this, null).getString()));
        return list;
    }

    private PoolBeanService getPoolBeanService() {
        return getWebadminApplication().getPoolBeanService();
    }

    private void getPoolsAction() {
        try {
            _log.debug("getPoolListAction called");
            this._poolBeans = getPoolBeanService().getPoolBeans();
        } catch (PoolBeanServiceException ex) {
            this.error(getStringResource("error.getPoolsFailed") + ex.getMessage());
            _log.debug("getPoolListAction failed {}", ex.getMessage());
            this._poolBeans = null;
        }
    }

    private class PoolUsageForm extends Form {

        public PoolUsageForm(String id) {
            super(id);
            Button button = new Button("submit");
            MetaDataRoleAuthorizationStrategy.authorize(button, ENABLE, Role.ADMIN);
            _log.debug("submit isEnabled : {}", String.valueOf(button.isEnabled()));
            this.add(button);
        }

        @Override
        protected void onSubmit() {
            _log.debug("button pressed");
            if (_poolBeans != null && _selectedOption != null) {
                try {
                    _log.debug("selected: {}", _selectedOption.getValue());
                    PoolV2Mode poolMode = new PoolV2Mode(_selectedOption.getKey());
                    getPoolBeanService().changePoolMode(_poolBeans, poolMode,
                            getWebadminSession().getUserName());
                    getPoolsAction();
                } catch (PoolBeanServiceException ex) {
                    _log.error("something went wrong with enable/disable");
                    this.error(getStringResource("error.changePoolModeFailed") + ex.getMessage());
                }
            }
        }
    }
}
