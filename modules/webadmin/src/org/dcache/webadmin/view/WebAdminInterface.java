package org.dcache.webadmin.view;

import org.apache.wicket.Page;
import org.apache.wicket.protocol.http.WebApplication;
import org.dcache.webadmin.controller.PoolBeanService;
import org.dcache.webadmin.view.pages.dcacheservices.DCacheServices;

/**
 * This is the main application object for the whole Webadmin-Interface
 * @author jans
 */
public class WebAdminInterface extends WebApplication {

    private PoolBeanService _poolBeanService;

    @Override
    public Class<? extends Page> getHomePage() {
        return DCacheServices.class;
    }

    public void setPoolBeanService(PoolBeanService poolBeanService) {
        this._poolBeanService = poolBeanService;
    }

    public PoolBeanService getPoolBeanService() {
        return _poolBeanService;
    }
}
