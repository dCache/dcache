package org.dcache.webadmin.model.businessobjects;

import diskCacheV111.vehicles.RestoreHandlerInfo;

/**
 *
 * @author jans
 */
public class RestoreInfo {

    private final RestoreHandlerInfo restoreHandler;
    private final String pnfsId;

    public RestoreInfo(RestoreHandlerInfo restoreHandler) {
        this.restoreHandler = restoreHandler;
        this.pnfsId = restoreHandler.getName().substring(0,
                restoreHandler.getName().indexOf('@'));
    }

    public String getPnfsId() {
        return pnfsId;
    }

    public RestoreHandlerInfo getRestoreHandler() {
        return restoreHandler;
    }
}
