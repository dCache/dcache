package org.dcache.webadmin.view.pages.spacetokens.spacereservationpanel;

import java.util.ArrayList;
import java.util.List;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.view.pages.spacetokens.beans.SpaceReservationBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.EvenOddListView;

/**
 * @author jans
 */
public class SpaceReservationPanel extends BasePanel {

    private static final String SPECIAL_LINKGROUP_HEADER = "header";
    private List<SpaceReservationBean> _spaceReservations =
            new ArrayList<SpaceReservationBean>();
    private String _selectedGroup = (String) getStringResource(
            SPECIAL_LINKGROUP_HEADER).getObject();

    public SpaceReservationPanel(String id, String linkGroupName) {
        super(id);
        _selectedGroup += linkGroupName;
        add(new Label("spaceReservationHeader",
                new PropertyModel<String>(this, "_selectedGroup")));
        add(new EvenOddListView<SpaceReservationBean>("spaceReservationPanelListview",
                _spaceReservations) {

            @Override
            protected void populateItem(ListItem<SpaceReservationBean> item) {
                SpaceReservationBean spaceReservation = item.getModelObject();
                item.add(new Label("id", spaceReservation.getId()));
                item.add(new Label("description", spaceReservation.getDescription()));
                item.add(new Label("linkGroupName", spaceReservation.getLinkGroupRef()));
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
                item.add(new Label("lifetime", spaceReservation.getLifetime()));
                item.add(new Label("expiration", spaceReservation.getExpiration()));
            }
        });
    }

    public void setReservations(List<SpaceReservationBean> reservations) {
        if (reservations != null) {
            _spaceReservations = reservations;
        }
    }
}
