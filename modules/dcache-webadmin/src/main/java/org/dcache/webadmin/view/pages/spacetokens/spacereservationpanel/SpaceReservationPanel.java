package org.dcache.webadmin.view.pages.spacetokens.spacereservationpanel;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.model.StringResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import org.dcache.webadmin.view.pages.spacetokens.beans.SpaceReservationBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.EvenOddListView;

/**
 * @author jans
 */
public class SpaceReservationPanel extends BasePanel {

    private static final String SPECIAL_LINKGROUP_HEADER = "header";
    private static final long serialVersionUID = -6726351719532766705L;
    private String _headerLinkgroup = "";
    private static final Logger _log = LoggerFactory.getLogger(SpaceReservationPanel.class);

    public SpaceReservationPanel(String id, IModel<? extends List<SpaceReservationBean>> model) {
        super(id);
        add(new Label("spaceReservationHeader",
                new StringResourceModel(SPECIAL_LINKGROUP_HEADER, this, null)));
        add(new Label("headerLinkgroup",
                new PropertyModel(this, "_headerLinkgroup")));
        add(new EvenOddListView<SpaceReservationBean>("spaceReservationPanelListview",
                model) {

            private static final long serialVersionUID = -1953789401431035083L;

            @Override
            protected void populateItem(ListItem<SpaceReservationBean> item) {
                SpaceReservationBean spaceReservation = item.getModelObject();
                item.add(new Label("id", spaceReservation.getId()));
                item.add(new Label("description", spaceReservation.getDescription()));
                item.add(new Label("linkGroupId", Long.toString(spaceReservation.getLinkGroupRef())));
                item.add(new Label("storage", spaceReservation.getStorage()));
                item.add(new Label("vogroup", spaceReservation.getVogroup()));
                item.add(new Label("state", spaceReservation.getState()));
                item.add(new Label("size", Long.valueOf(
                        spaceReservation.getSize()).toString()));
                item.add(new Label("used", Long.valueOf(
                        spaceReservation.getUsedSpace()).toString()));
                item.add(new Label("allocated", Long.valueOf(
                        spaceReservation.getAllocatedSpace()).toString()));
                item.add(new Label("created", spaceReservation.getCreated()));
            }
        });
    }

    public void setLinkGroupName(String linkGroupName) {
        if (linkGroupName != null) {
            _headerLinkgroup = linkGroupName;
        }
    }
}
