package org.dcache.webadmin.view.pages.tapetransferqueue;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import org.dcache.webadmin.controller.TapeTransfersService;
import org.dcache.webadmin.controller.exceptions.TapeTransfersServiceException;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.pages.tapetransferqueue.beans.RestoreBean;
import org.dcache.webadmin.view.util.EvenOddListView;

/**
 *
 * @author jans
 */
public class TapeTransferQueue extends BasePage {

    private static final long serialVersionUID = 8313857084027604473L;
    private List<RestoreBean> _restoreBeans;
    private static final Logger _log = LoggerFactory.getLogger(TapeTransferQueue.class);

    public TapeTransferQueue() {
        add(new FeedbackPanel("feedback"));
        ListView<RestoreBean> listview =
                new EvenOddListView<RestoreBean>("TapeTransferQueueListview",
                new PropertyModel(this, "_restoreBeans")) {

                    private static final long serialVersionUID = 9166078572922366382L;

                    @Override
                    protected void populateItem(ListItem item) {
                        RestoreBean restore = (RestoreBean) item.getModelObject();
                        item.add(new Label("pnfsid",
                                new PropertyModel<RestoreBean>(restore, "_pnfsId")));
                        item.add(new Label("subnet",
                                new PropertyModel<RestoreBean>(restore, "_subnet")));
                        item.add(new Label("candidate",
                                new PropertyModel<RestoreBean>(restore, "_pool")));
                        item.add(new Label("started",
                                new PropertyModel<RestoreBean>(restore, "_startTime")));
                        item.add(new Label("clients",
                                new PropertyModel<RestoreBean>(restore, "_clients")));
                        item.add(new Label("retries",
                                new PropertyModel<RestoreBean>(restore, "_retries")));
                        item.add(new Label("status",
                                new PropertyModel<RestoreBean>(restore, "_status")));
                    }
                };
        add(listview);
        getRestoresAction();
    }

    private TapeTransfersService getTapeTransferService() {
        return getWebadminApplication().getTapeTransfersService();
    }

    private void getRestoresAction() {
        try {
            _log.debug("getRestoresAction called");
            _restoreBeans = getTapeTransferService().getRestores();
        } catch (TapeTransfersServiceException ex) {
            this.error(getStringResource("error.getRestoresFailed") + ex.getMessage());
            _log.debug("getRestoresAction failed {}", ex.getMessage());
            _restoreBeans = null;
        }
    }
}
