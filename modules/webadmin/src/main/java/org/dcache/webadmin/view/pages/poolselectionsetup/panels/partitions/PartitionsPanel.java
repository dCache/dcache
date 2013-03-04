package org.dcache.webadmin.view.pages.poolselectionsetup.panels.partitions;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.model.IModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PartitionsBean;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;

/**
 *
 * @author jans
 */
public class PartitionsPanel extends BasePanel {

    private static final Logger _log = LoggerFactory.getLogger(PartitionsPanel.class);
    private static final long serialVersionUID = -2606040316145565815L;
    private final IModel<List<PartitionsBean>> _model;

    public PartitionsPanel(String id, IModel<List<PartitionsBean>> partitions) {
        super(id);
        _model = partitions;
        addMarkup();
    }

    private void addMarkup() {

        ListView<PartitionsBean> partitionNames =
                new ListView<PartitionsBean>("partitionNames", _model.getObject()) {

                    private static final long serialVersionUID = -872567003908613003L;

                    @Override
                    protected void populateItem(ListItem<PartitionsBean> item) {
                        item.add(new Label("partitionName", item.getModelObject().getPartitionName()));
                    }
                };
        add(partitionNames);
        PartitionsBean firstPartitionsBean = null;
        if (_model != null && _model.getObject() != null) {
            firstPartitionsBean = _model.getObject().get(0);
        }
        List<String> keys;
        if (firstPartitionsBean != null) {
            keys = new ArrayList<>(firstPartitionsBean.getProperties().keySet());
        } else {
            keys = Collections.emptyList();
        }

        ListView<String> rows = new ListView<String>("keyRows", keys) {

            private static final long serialVersionUID = -3882120426084480841L;

            @Override
            protected void populateItem(ListItem<String> item) {
                item.add(new KeyValueFragment("keyValuePairs", item.getModelObject()));
            }
        };
        add(rows);

    }

    private class KeyValueFragment extends Fragment {

        private static final long serialVersionUID = -7677472115011654762L;

        public KeyValueFragment(String id, final String key) {
            super(id, "keyValueFragment", PartitionsPanel.this);
            add(new Label("key", key));
            ListView<PartitionsBean> columns =
                    new ListView<PartitionsBean>("keyColumns",
                    _model.getObject()) {

                        private static final long serialVersionUID = -4903201605492027439L;

                        @Override
                        protected void populateItem(ListItem<PartitionsBean> item) {
                            PartitionsBean partition = item.getModelObject();
                            item.add(new Label("value", partition.getProperties().get(key)));
                        }
                    };
            add(columns);

        }
    }
}
