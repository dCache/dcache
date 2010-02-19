package org.dcache.webadmin.controller;

import java.util.List;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.view.beans.PoolBean;

/**
 * Services for the view to invoke
 * @author jans
 */
public interface PoolBeanService {

    public List<PoolBean> getPoolBeans() throws PoolBeanServiceException;
}
