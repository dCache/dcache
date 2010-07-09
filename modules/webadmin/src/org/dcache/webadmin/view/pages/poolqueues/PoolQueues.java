package org.dcache.webadmin.view.pages.poolqueues;

import java.util.ArrayList;
import java.util.List;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.controller.PoolQueuesService;
import org.dcache.webadmin.controller.exceptions.PoolQueuesServiceException;
import org.dcache.webadmin.view.beans.PoolGroupBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.poolQueuesPanel.PoolQueuesPanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pool Request Queues Page
 * @author jans
 */
public class PoolQueues extends BasePage implements AuthenticatedWebPage {

    private static final Logger _log = LoggerFactory.getLogger(PoolQueues.class);
    private PoolGroupBean _allPoolsGroup = new PoolGroupBean("all",
            new ArrayList<PoolSpaceBean>(), new ArrayList<PoolQueueBean>());

    public PoolQueues() {
        add(new PoolQueuesPanel("poolQueuesPanel",
                new PropertyModel<PoolGroupBean>(this, "_allPoolsGroup")));
        getPoolQueuesAction();
    }

    private PoolQueuesService getPoolQueuesService() {
        return getWebadminApplication().getPoolQueuesService();
    }

    private void getPoolQueuesAction() {
        try {
            _log.debug("getPoolQueuesAction called");
            setPoolQueues(getPoolQueuesService().getPoolQueues());
        } catch (PoolQueuesServiceException ex) {
            this.error(getStringResource("error.getPoolsQueuesFailed") + ex.getMessage());
            _log.debug("getPoolQueuesAction failed {}", ex.getMessage());
            setPoolQueues(null);
        }
    }

    private void setPoolQueues(List<PoolQueueBean> poolQueues) {
        _allPoolsGroup.setPoolQueues(poolQueues);
    }
}
