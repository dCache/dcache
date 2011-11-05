package org.dcache.webadmin.view.panels.poolQueuesPanel;

import java.util.List;
import org.apache.wicket.model.IModel;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.view.beans.PoolGroupBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolRequestQueue;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reusable Panel for the Listview on Poolqueues
 * @author jans
 */
public class PoolQueuesPanel extends BasePanel {

    private IModel<PoolGroupBean> _poolModel;
    private static final Logger _log = LoggerFactory.getLogger(PoolQueuesPanel.class);

    public PoolQueuesPanel(String id, IModel<PoolGroupBean> model) {
        super(id);
        _poolModel = model;
//        _log.debug(_poolModel.getObject().getAllPoolQueueNames().toString());
        add(new ListView("tableHeaderView", getAllPoolQueueNames()) {

            @Override
            protected void populateItem(ListItem item) {
                String queueName = (String) item.getModelObject();
                item.add(new Label("requestQueueName", queueName));
            }
        });
        add(new ListView("headerFragmentView", getAllPoolQueueNames()) {

            @Override
            protected void populateItem(ListItem item) {
                item.add(new HeaderFragment("colheader"));
            }
        });
        ListView view = new EvenOddListView("poolsListview",
                getPoolQueues()) {

            @Override
            protected void populateItem(ListItem item) {
                item.add(new PoolFragment("pool", item.getModel()));
            }
        };
        add(view);
        add(new PoolFragment("total", getTotalQueue()));
    }

    private PropertyModel<String> getAllPoolQueueNames() {
        return new PropertyModel<String>(_poolModel, "_allPoolQueueNames");
    }

    private PropertyModel<List<PoolQueueBean>> getPoolQueues() {
        return new PropertyModel<List<PoolQueueBean>>(_poolModel, "_poolQueues");
    }

    private PropertyModel<PoolQueueBean> getTotalQueue() {
        return new PropertyModel<PoolQueueBean>(_poolModel, "_totalMovers");
    }

    private class HeaderFragment extends Fragment {

        public HeaderFragment(String id) {
            super(id, "headerFragment", PoolQueuesPanel.this);
        }
    }

    private class QueueFragment extends Fragment {

        public QueueFragment(String id, PoolRequestQueue requestQueue) {
            super(id, "queueFragment", PoolQueuesPanel.this);
            this.add(new Label("active", new PropertyModel(requestQueue, "active")));
            this.add(new Label("max", new PropertyModel(requestQueue, "max")));
            this.add(new Label("queued", new PropertyModel(requestQueue, "queued")));
        }
    }

    private class PoolFragment extends Fragment {

        private IModel<PoolQueueBean> _queue;

        public PoolFragment(String id, IModel<PoolQueueBean> model) {
            super(id, "poolFragment", PoolQueuesPanel.this);
            _queue = model;
            this.add(new Label("poolName", new PropertyModel(model, "name")));
            this.add(new Label("pooldomainName", new PropertyModel(model,
                    "domainName")));
            this.add(new ListView("poolMoversView", getAllPoolQueueNames()) {

                @Override
                protected void populateItem(ListItem item) {
                    String currentMover = (String) item.getModelObject();
                    PoolRequestQueue queue = _queue.getObject().
                            getMoverQueues().get(currentMover);
                    if (queue == null) {
                        queue = new PoolRequestQueue();
                    }
                    item.add(new QueueFragment("movers", queue));
                }
            });
        }
    }
}
