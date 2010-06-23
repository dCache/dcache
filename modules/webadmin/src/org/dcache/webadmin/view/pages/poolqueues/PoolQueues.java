package org.dcache.webadmin.view.pages.poolqueues;

import java.util.ArrayList;
import java.util.List;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Fragment;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolRequestQueue;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.util.EvenOddListView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pool Request Queues Page
 * @author jans
 */
public class PoolQueues extends BasePage implements AuthenticatedWebPage {

    private static final int NUMBER_OF_QUEUES = 7;
    private static final Logger _log = LoggerFactory.getLogger(PoolQueues.class);
    private List<PoolQueueBean> _poolQueues;
    private PoolQueueBean _total = new PoolQueueBean();

    public PoolQueues() {
        getPoolQueuesAction();
        for (int columnNumber = 1; columnNumber <= NUMBER_OF_QUEUES; columnNumber++) {
            add(new HeaderFragment("colheader" + columnNumber));
        }
        ListView view = new EvenOddListView("listview", _poolQueues) {

            @Override
            protected void populateItem(ListItem item) {
                PoolQueueBean poolQueue = (PoolQueueBean) item.getModelObject();
                item.add(new PoolFragment("pool", poolQueue));
            }
        };
        add(view);
        add(new PoolFragment("total", _total));
    }

    private void getPoolQueuesAction() {
        _log.debug("getPoolQueuesAction called");
//        TODO implementation and invocation of real service
        _poolQueues = new ArrayList<PoolQueueBean>();
        PoolQueueBean blub = new PoolQueueBean();
        blub.setName("pool");
        blub.setDomainName("blubdomain");
        blub.setMovers(new PoolRequestQueue(5, 6, 7));
        _poolQueues.add(blub);
        calculateTotal();
    }

    private void calculateTotal() {
        if (_poolQueues != null) {
            for (PoolQueueBean queue : _poolQueues) {
                _total.addPoolQueue(queue);
            }
        } else {
            _total = new PoolQueueBean();
        }
        _total.setName("Total");
        _total.setDomainName("Total");
    }

    private class HeaderFragment extends Fragment {

        public HeaderFragment(String id) {
            super(id, "headerFragment", PoolQueues.this);
        }
    }

    private class QueueFragment extends Fragment {

        public QueueFragment(String id, PoolRequestQueue requestQueue) {
            super(id, "queueFragment", PoolQueues.this);
            add(new Label("active", new PropertyModel(requestQueue, "active")));
            add(new Label("max", new PropertyModel(requestQueue, "max")));
            add(new Label("queued", new PropertyModel(requestQueue, "queued")));
        }
    }

    private class PoolFragment extends Fragment {

        public PoolFragment(String id, PoolQueueBean poolQueue) {
            super(id, "poolFragment", PoolQueues.this);
            add(new Label("name", poolQueue.getName()));
            add(new Label("domainName", poolQueue.getDomainName()));
            add(new QueueFragment("movers", poolQueue.getMovers()));
            add(new QueueFragment("restores", poolQueue.getRestores()));
            add(new QueueFragment("stores", poolQueue.getStores()));
            add(new QueueFragment("p2pserver", poolQueue.getP2pserver()));
            add(new QueueFragment("p2pclient", poolQueue.getP2pclient()));
            add(new QueueFragment("p2p", poolQueue.getP2p()));
            add(new QueueFragment("regular", poolQueue.getRegular()));
        }
    }
}
