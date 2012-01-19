package org.dcache.webadmin.view.pages.poolselectionsetup.panels.simulatediorequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.data.GridView;
import org.apache.wicket.markup.repeater.data.IDataProvider;
import org.apache.wicket.markup.repeater.data.ListDataProvider;
import org.apache.wicket.model.AbstractReadOnlyModel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.controller.exceptions.PoolSelectionSetupServiceException;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.poolselectionsetup.PoolSelectionSetup;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.IORequest;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.MatchBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class SimulatedIORequestPanel extends BasePanel {

    private static final String EMPTY_STRING = "";
    private static final Logger _log = LoggerFactory.getLogger(SimulatedIORequestPanel.class);
    private IoDirections _ioDirection = IoDirections.READ;
    private String _linkGroup = "none";
    private String _store = "*";
    private String _dCache = "*";
    private String _net = "*";
    private String _protocol = "DCap/3";
    private List<MatchBean> _matches = Collections.EMPTY_LIST;
    private PoolSelectionSetup _motherPage;

    public SimulatedIORequestPanel(String id, PoolSelectionSetup motherPage) {
        super(id);
        _motherPage = motherPage;
        Form form = new Form("simulatediorequestpanelForm");
        form.add(new SubmitButton("submit"));
        TextField linkGroupInput = new TextField("linkgroup",
                new PropertyModel(this, "_linkGroup"));
        form.add(linkGroupInput);
        DropDownChoice ioDirection = new DropDownChoice("iodirection",
                new PropertyModel(this, "_ioDirection"),
                new IoDirectionsModel());
        form.add(ioDirection);
        TextField storeInput = new TextField("store",
                new PropertyModel(this, "_store"));
        form.add(storeInput);
        TextField dCacheInput = new TextField("dcache",
                new PropertyModel(this, "_dCache"));
        form.add(dCacheInput);
        TextField netInput = new TextField("net",
                new PropertyModel(this, "_net"));
        form.add(netInput);
        TextField protocolInput = new TextField("protocol",
                new PropertyModel(this, "_protocol"));
        form.add(protocolInput);
        ListView<MatchBean> matchView = new ListView<MatchBean>("matches",
                new PropertyModel<List<MatchBean>>(this, "_matches")) {

            @Override
            protected void populateItem(ListItem<MatchBean> item) {
                MatchFragment matchFragment = new MatchFragment(
                        "levelFragment", item.getModel(),
                        item.getIndex());
                item.add(matchFragment);
            }
        };
        form.add(matchView);
        add(form);

    }

    private class IoDirectionsModel extends AbstractReadOnlyModel<List<IoDirections>> {

        @Override
        public List<IoDirections> getObject() {
            return Arrays.asList(IoDirections.values());
        }
    }

    private class MatchFragment extends Fragment {

        private static final int POOLS_PER_ROW = 8;

        public MatchFragment(String id, IModel<MatchBean> model, int level) {
            super(id, "matchFragment");
            String matchLevel;
            if (model.getObject().getTag() == null) {
                matchLevel = String.valueOf(level);
            } else {
                matchLevel = model.getObject().getTag() + String.valueOf(level);
            }

            add(new Label("matchLevel", matchLevel));
            List<String> pools = model.getObject().getPoolList();
            IDataProvider<String> dataProvider =
                    new ListDataProvider<String>(pools);
            GridView<String> grid = new GridView<String>("pools",
                    dataProvider) {

                @Override
                protected void populateEmptyItem(Item<String> item) {
                    Link link = new Link("link") {

                        @Override
                        public void onClick() {
//                           do nothing is on purpose - just an empty column
                        }
                    };
                    link.add(new Label("name", EMPTY_STRING));
                    item.add(link);
                }

                @Override
                protected void populateItem(final Item<String> item) {
                    Link link = _motherPage.getLinkToPool("link", item.getModelObject());
                    link.add(new Label("name", item.getModelObject()));
                    item.add(link);
                }
            };
            grid.setColumns(Math.min(POOLS_PER_ROW, pools.size()));
            add(grid);
        }
    }

    private class SubmitButton extends Button {

        public SubmitButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            IORequest request = new IORequest(_ioDirection, _store, _dCache,
                    _net, _protocol, _linkGroup);
            try {
                _matches = ((WebAdminInterface) getApplication()).getPoolSelectionSetupService().
                        getMatchForIORequest(request);
            } catch (PoolSelectionSetupServiceException ex) {
                error("Match failed: " + ex.getCause().getMessage());
            }
        }
    }
}
