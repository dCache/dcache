package org.dcache.webadmin.view.pages.poolselectionsetup.panels.simulatediorequest;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolSelectionSetupServiceException;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.poolselectionsetup.PoolSelectionSetup;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.IORequest;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.MatchBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;

/**
 *
 * @author jans
 */
public class SimulatedIORequestPanel extends BasePanel {

    private static final String EMPTY_STRING = "";
    private static final Logger _log = LoggerFactory.getLogger(SimulatedIORequestPanel.class);
    private static final long serialVersionUID = 4415717034917037746L;
    private IoDirections _ioDirection = IoDirections.READ;
    private String _linkGroup = "none";
    private String _store = "*";
    private String _dCache = "*";
    private String _net = "*";
    private String _protocol = "DCap/3";
    private List<MatchBean> _matches = Collections.emptyList();
    private PoolSelectionSetup _motherPage;

    public SimulatedIORequestPanel(String id, PoolSelectionSetup motherPage) {
        super(id);
        _motherPage = motherPage;
        Form form = new Form("simulatediorequestpanelForm");
        form.add(new SubmitButton("submit"));
        TextField linkGroupInput = new TextField("linkgroup",
                new PropertyModel(this, "_linkGroup"));
        form.add(linkGroupInput);
        DropDownChoice<IoDirections> ioDirection = new DropDownChoice<>("iodirection",
                new PropertyModel<IoDirections>(this, "_ioDirection"),
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

            private static final long serialVersionUID = 4873863555497361648L;

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

        private static final long serialVersionUID = 1826526610558013386L;

        @Override
        public List<IoDirections> getObject() {
            return Arrays.asList(IoDirections.values());
        }
    }

    private class MatchFragment extends Fragment {

        private static final int POOLS_PER_ROW = 8;
        private static final long serialVersionUID = 9138958292080879576L;

        public MatchFragment(String id, IModel<MatchBean> model, int level) {
            super(id, "matchFragment", SimulatedIORequestPanel.this);
            String matchLevel;
            if (model.getObject().getTag() == null) {
                matchLevel = String.valueOf(level);
            } else {
                matchLevel = model.getObject().getTag() + String.valueOf(level);
            }

            add(new Label("matchLevel", matchLevel));
            List<String> pools = model.getObject().getPoolList();
            IDataProvider<String> dataProvider =
                    new ListDataProvider<>(pools);
            GridView<String> grid = new GridView<String>("pools",
                    dataProvider) {

                private static final long serialVersionUID = -712827131026118042L;

                @Override
                protected void populateEmptyItem(Item<String> item) {
                    Link link = new Link("link") {

                        private static final long serialVersionUID = -8118543568666543834L;

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
            grid.setColumns(Math.min(POOLS_PER_ROW, pools.size() > 0 ? pools.size() : 1));
            add(grid);
        }
    }

    private class SubmitButton extends Button {

        private static final long serialVersionUID = 6917004473319676628L;

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
