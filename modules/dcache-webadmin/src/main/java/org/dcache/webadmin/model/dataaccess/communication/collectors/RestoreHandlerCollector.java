package org.dcache.webadmin.model.dataaccess.communication.collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.RestoreRequestsReceiver;

import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;

/**
 * @author jans
 */
public class RestoreHandlerCollector extends Collector {
    private static final Logger _log
                    = LoggerFactory.getLogger(RestoreHandlerCollector.class);
    private RestoreRequestsReceiver receiver;

    @Override
    public Status call() throws InterruptedException {
        _pageCache.put(ContextPaths.RESTORE_INFOS,
                       (receiver.getAllRequests().stream()
                                .map(RestoreInfo::new)
                                .collect(Collectors.toSet())));
        return Status.SUCCESS;
    }

    @Override
    public void initialize() {
        receiver.initialize();
        super.initialize();
    }

    @Required
    public void setReceiver(RestoreRequestsReceiver receiver) {
        this.receiver = receiver;
    }
}
