package org.dcache.webadmin.view.pages.poollist;

import diskCacheV111.pools.PoolV2Mode;
import java.util.List;
import java.util.ArrayList;
import org.apache.wicket.AttributeModifier;
import org.apache.wicket.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.ChoiceRenderer;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.model.StringResourceModel;
import org.dcache.webadmin.controller.PoolBeanService;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.view.util.CustomLink;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.PoolBean;
import org.dcache.webadmin.view.beans.SelectOption;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.util.Role;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * The PoolUsage Webpage
 * @author jans
 */
public class PoolList extends AuthenticatedWebPage {

    private static final int DEFAULT_DROP_DOWN_CHOICE = 0;
    private List<PoolBean> _poolBeans;
    private SelectOption _selectedOption;
    private static final Logger _log = LoggerFactory.getLogger(PoolList.class);

    public PoolList() {
        Form headerForm = new PoolUsageForm("poolUsageForm");
        headerForm.add(new CustomLink("homeLink", getApplication().getHomePage()));
        headerForm.add(createPoolModeDropDown("mode"));
        headerForm.add(new FeedbackPanel("feedback"));
        getPoolsAction();
        ListView poolListView = new PoolBeanListView("listview",
                new PropertyModel(this, "_poolBeans"));
        headerForm.add(poolListView);
        add(headerForm);
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

    private String getErrorMessage(String resourceKey) {
        return new StringResourceModel(resourceKey, this, null).getString();
    }

    private PoolBeanService getPoolBeanService() {
        return ((WebAdminInterface) getApplication()).getPoolBeanService();
    }

    private void getPoolsAction() {
        try {
            _log.debug("getPoolListAction called");
            this._poolBeans = getPoolBeanService().getPoolBeans();
        } catch (PoolBeanServiceException ex) {
            this.error(getErrorMessage("error.getPoolsFailed") + ex.getMessage());
            _log.debug("getPoolListAction failed {}", ex.getMessage());
            this._poolBeans = null;
        }
    }

    private class PoolUsageForm extends Form {

        public PoolUsageForm(String id) {
            super(id);
            SubmitButton button = new SubmitButton("submit");
            MetaDataRoleAuthorizationStrategy.authorize(button, ENABLE, Role.ADMIN);
            this.add(button);
            _log.debug("isEnabled : {}", String.valueOf(button.isEnabled()));
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
                    this.error(getErrorMessage("error.changePoolModeFailed") + ex.getMessage());
                }
            }
        }
    }

    private class SubmitButton extends Button {

        public SubmitButton(String id) {
            super(id);
        }
    }

    private class PoolBeanListView extends EvenOddListView<PoolBean> {

        PoolBeanListView(String id, IModel<? extends List<PoolBean>> model) {
            super(id, model);
        }

        @Override
        protected void populateItem(final ListItem<PoolBean> item) {
            PoolBean poolBean = item.getModelObject();
            item.add(new CheckBox("selected", new PropertyModel<Boolean>(poolBean, "selected")));
            item.add(new Label("name", poolBean.getName()));
            item.add(new Label("domainName", poolBean.getDomainName()));
            item.add(new Label("enabled", new Boolean(poolBean.isEnabled()).toString()));
            item.add(new Label("totalSpace", new Long(poolBean.getTotalSpace()).toString()));
            item.add(new Label("freeSpace", new Long(poolBean.getFreeSpace()).toString()));
            item.add(new Label("preciousSpace", new Long(poolBean.getPreciousSpace()).toString()));
            setPercentages(item, poolBean);
        }

        private void setPercentages(ListItem item, PoolBean poolBean) {
            String preciousStyle = buildStyleAttribute(poolBean.getPercentagePrecious());
            setStyleAttribute(item, "percentagePrecious", preciousStyle);
            String usedStyle = buildStyleAttribute(poolBean.getPercentageUsed());
            setStyleAttribute(item, "percentageUsed", usedStyle);
            String freeStyle = buildStyleAttribute(poolBean.getPercentageFree());
            setStyleAttribute(item, "percentageFree", freeStyle);
        }

        private String buildStyleAttribute(float width) {
            String style = "width: " + (new Float(width).toString()) + "%";
            return style;
        }

        private void setStyleAttribute(ListItem item, String markupContainerName, String styleAttribute) {
            WebMarkupContainer layoutLabel = new WebMarkupContainer(markupContainerName);
            layoutLabel.add(new AttributeModifier("style",
                    new Model(styleAttribute)));
            item.add(layoutLabel);
        }
    }
}
