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
    private int _httpPort;
    private int _httpsPort;
    private String _dcacheName;

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

    public void setDcacheName(String dCacheName) {
        _dcacheName = dCacheName;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public void setHttpPort(int httpPort) {
        _httpPort = httpPort;
    }

    public void setHttpsPort(int httpsPort) {
        _httpsPort = httpsPort;
    }
}
