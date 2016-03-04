package org.dcache.webadmin.view.panels.poolQueuesPanel;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;

import java.util.List;

import org.dcache.webadmin.view.beans.PoolGroupBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolRequestQueue;
import org.dcache.webadmin.view.panels.basepanel.BasePanel;
import org.dcache.webadmin.view.util.EvenOddListView;

/**
 * Reusable Panel for the Listview on Poolqueues
 * @author jans
 */
public class PoolQueuesPanel extends BasePanel {

    private static final long serialVersionUID = -6513684957808201860L;
    private IModel<PoolGroupBean> _poolModel;

    public PoolQueuesPanel(String id, IModel<PoolGroupBean> model) {
        super(id);
        _poolModel = model;
//        _log.debug(_poolModel.getObject().getAllPoolQueueNames().toString());
        add(new ListView("tableHeaderView", getAllPoolQueueNames()) {

            private static final long serialVersionUID = -7007606196066597432L;

            @Override
            protected void populateItem(ListItem item) {
                String queueName = (String) item.getModelObject();
                item.add(new Label("requestQueueName", queueName));
            }
        });
        add(new ListView("headerFragmentView", getAllPoolQueueNames()) {

            private static final long serialVersionUID = -7308943127160766509L;

            @Override
            protected void populateItem(ListItem item) {
                item.add(new HeaderFragment("colheader"));
            }
        });
        ListView view = new EvenOddListView("poolsListview",
                getPoolQueues()) {

            private static final long serialVersionUID = -5557315666867448972L;

            @Override
            protected void populateItem(ListItem item) {
                item.add(new PoolFragment("pool", item.getModel()));
            }
        };
        add(view);
        add(new PoolFragment("total", getTotalQueue()));
    }

    private PropertyModel<String> getAllPoolQueueNames() {
        return new PropertyModel<>(_poolModel, "_allPoolQueueNames");
    }

    private PropertyModel<List<PoolQueueBean>> getPoolQueues() {
        return new PropertyModel<>(_poolModel, "_poolQueues");
    }

    private PropertyModel<PoolQueueBean> getTotalQueue() {
        return new PropertyModel<>(_poolModel, "_totalMovers");
    }

    private class HeaderFragment extends Fragment {

        private static final long serialVersionUID = 4855631838030754485L;

        public HeaderFragment(String id) {
            super(id, "headerFragment", PoolQueuesPanel.this);
        }
    }

    private class EmptyStringPropertyModel extends PropertyModel<Object> {

        private static final long serialVersionUID = 7296305267768847448L;

        public EmptyStringPropertyModel(Object modelObject, String expression) {
            super(modelObject, expression);
        }

        @Override
        public String getObject() {
            Object obj = super.getObject();
            if (obj instanceof Integer && ((Integer)obj) < 0) {
                return "";
            }
            return obj.toString();
        }
    }

    private class QueueFragment extends Fragment {

        private static final long serialVersionUID = -8844234902202594916L;

        public QueueFragment(String id, PoolRequestQueue requestQueue) {
            super(id, "queueFragment", PoolQueuesPanel.this);
            this.add(new Label("active", new PropertyModel(requestQueue, "active")));
            this.add(new Label("max", new EmptyStringPropertyModel(requestQueue, "max")));
            this.add(new Label("queued", new PropertyModel(requestQueue, "queued")));
        }
    }

    private class PoolFragment extends Fragment {

        private static final long serialVersionUID = -6317570570466714446L;
        private IModel<PoolQueueBean> _queue;

        public PoolFragment(String id, IModel<PoolQueueBean> model) {
            super(id, "poolFragment", PoolQueuesPanel.this);
            _queue = model;
            this.add(new Label("poolName", new PropertyModel(model, "name")));
            this.add(new Label("pooldomainName", new PropertyModel(model,
                    "domainName")));
            this.add(new ListView("poolMoversView", getAllPoolQueueNames()) {

                private static final long serialVersionUID = -918341847039679266L;

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
