package org.dcache.webadmin.view.pages.tapetransferqueue;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import org.dcache.webadmin.controller.exceptions.TapeTransfersServiceException;
import org.dcache.webadmin.view.pages.basepage.SortableBasePage;
import org.dcache.webadmin.view.pages.tapetransferqueue.beans.RestoreBean;
import org.dcache.webadmin.view.util.EvenOddListView;

/**
 *
 * @author jans
 */
public class TapeTransferQueue extends SortableBasePage {

    private static final long serialVersionUID = 8313857084027604473L;
    private static final Logger _log = LoggerFactory.getLogger(TapeTransferQueue.class);

    final List<RestoreBean> _restoreBeans;

    public TapeTransferQueue() {
        Form<?> form = new Form<Void>("tapeTransferQueueForm");
        form.add(new FeedbackPanel("feedback"));
        _restoreBeans = getRestoreBeans();
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
        form.add(listview);
        add(form);
    }

    public List<RestoreBean> getRestoreBeans() {
        try {
            _log.debug("getRestoresAction called");
            return getWebadminApplication().getTapeTransfersService().getRestores();
        } catch (TapeTransfersServiceException ex) {
            this.error(getStringResource("error.getRestoresFailed") + ex.getMessage());
            _log.debug("getRestoresAction failed {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        addFilterSelectScript("tape", response);
    }
}
