package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolQueuesServiceException;
import org.dcache.webadmin.view.beans.PoolQueueBean;

/**
 * Services for a Page on PoolMoverQueues
 * @author jans
 */
public interface PoolQueuesService {

    List<PoolQueueBean> getPoolQueues() throws PoolQueuesServiceException;
}
