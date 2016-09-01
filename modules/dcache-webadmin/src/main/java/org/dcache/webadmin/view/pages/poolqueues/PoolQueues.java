package org.dcache.webadmin.view.pages.poolqueues;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.dcache.webadmin.controller.PoolQueuesService;
import org.dcache.webadmin.controller.exceptions.PoolQueuesServiceException;
import org.dcache.webadmin.view.beans.PoolGroupBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.pages.basepage.SortableBasePage;
import org.dcache.webadmin.view.panels.poolQueuesPanel.PoolQueuesPanel;

/**
 * The Pool Request Queues Page
 * @author jans
 */
public class PoolQueues extends SortableBasePage {

    private static final Logger _log = LoggerFactory.getLogger(PoolQueues.class);
    private static final long serialVersionUID = -6482302256752371950L;

    public PoolQueues() {
        Form<?> form = new Form<Void>("poolQueuesForm");
        form.add(new PoolQueuesPanel("poolQueuesPanel",
                        new PropertyModel<PoolGroupBean>(this, "allPoolsGroup")));
        add(form);
    }

    public PoolGroupBean getAllPoolsGroup() {
        PoolGroupBean allPoolsGroup = new PoolGroupBean("all",
                        new ArrayList<PoolSpaceBean>(),
                        new ArrayList<PoolQueueBean>());
        getPoolQueuesAction(allPoolsGroup);
        return allPoolsGroup;
    }

    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        addFilterSelectScript("queues", response);
    }

    private PoolQueuesService getPoolQueuesService() {
        return getWebadminApplication().getPoolQueuesService();
    }

    private void getPoolQueuesAction(PoolGroupBean bean) {
        List<PoolQueueBean> poolQueues;
        try {
            _log.debug("getPoolQueuesAction called");
            poolQueues = getPoolQueuesService().getPoolQueues();
        } catch (PoolQueuesServiceException ex) {
            this.error(getStringResource("error.getPoolsQueuesFailed") + ex.getMessage());
            _log.debug("getPoolQueuesAction failed {}", ex.getMessage());
            poolQueues = Collections.emptyList();
        }
        bean.setPoolQueues(poolQueues);
    }
}
