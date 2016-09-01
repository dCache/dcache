package org.dcache.webadmin.view.pages.spacetokens;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import org.dcache.webadmin.controller.exceptions.LinkGroupsServiceException;
import org.dcache.webadmin.view.pages.basepage.SortableBasePage;
import org.dcache.webadmin.view.pages.spacetokens.beans.LinkGroupBean;
import org.dcache.webadmin.view.pages.spacetokens.beans.SpaceReservationBean;
import org.dcache.webadmin.view.pages.spacetokens.spacereservationpanel.SpaceReservationPanel;
import org.dcache.webadmin.view.util.EvenOddListView;

/**
 * @author jans
 */
public class SpaceTokens extends SortableBasePage {

    private static final long serialVersionUID = -8335635306002254217L;
    private SpaceReservationPanel _spaceReservationsPanel =
            new SpaceReservationPanel("spaceReservationsPanel",
            new PropertyModel<List<SpaceReservationBean>>(
            this, "reservations"));
    private LinkGroupBean _currentLinkGroup;
    private static final Logger _log = LoggerFactory.getLogger(SpaceTokens.class);

    public SpaceTokens() {
        createMarkup();
    }

    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        addFilterSelectScript("tokens", response);
    }

    private void createMarkup() {
        Form<?> form = new Form<Void>("spaceTokensForm");
        form.add(new FeedbackPanel("feedback"));
        form.add(new LinkGroupListView("linkGroupView", new PropertyModel(this,
                "tokenInfo")));
        form.add(_spaceReservationsPanel);
        add(form);
    }

    public List<SpaceReservationBean> getReservations() {
        if (_currentLinkGroup == null) {
            return Collections.emptyList();
        }
        return _currentLinkGroup.getReservations();
    }

    public List<LinkGroupBean> getTokenInfo() {
        try {
            return getWebadminApplication().getLinkGroupsService().getLinkGroups();
        } catch (LinkGroupsServiceException ex) {
            this.error(getStringResource("error.getTokenInfoFailed") + ex.getMessage());
            _log.debug("getTokenInfo failed {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

    private class LinkGroupListView extends EvenOddListView<LinkGroupBean> {

        private static final long serialVersionUID = 7758280747018444858L;

        LinkGroupListView(String id, IModel<? extends List<LinkGroupBean>> model) {
            super(id, model);
        }

        @Override
        protected void populateItem(ListItem<LinkGroupBean> item) {
            final LinkGroupBean linkGroup = item.getModelObject();
            Link linkGroupName = new Link("linkGroupLink") {

                private static final long serialVersionUID = 5087459585392227658L;

                @Override
                public void onClick() {
                    _currentLinkGroup = linkGroup;
                    _spaceReservationsPanel.setLinkGroupName(linkGroup.getName());
                }
            };
            linkGroupName.add(new Label("nameMessage", linkGroup.getName()));
            item.add(linkGroupName);
            item.add(new Label("id", Long.toString(linkGroup.getId())));
            item.add(new Label("allowed", linkGroup.getAllowed()));
            item.add(new Label("vo", linkGroup.getVos()));
            item.add(new Label("availableSpace", Long.valueOf(
                    linkGroup.getAvailable()).toString()));
            item.add(new Label("reservedSpace", Long.valueOf(
                    linkGroup.getReserved()).toString()));
            item.add(new Label("freeSpace", Long.valueOf(
                    linkGroup.getFree()).toString()));
            item.add(new Label("totalSpace", Long.valueOf(
                    linkGroup.getTotal()).toString()));
        }
    }
}
