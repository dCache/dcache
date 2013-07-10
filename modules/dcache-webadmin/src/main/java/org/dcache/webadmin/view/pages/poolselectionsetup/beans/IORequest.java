package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import org.dcache.webadmin.view.pages.poolselectionsetup.panels.simulatediorequest.IoDirections;

/**
 *
 * @author jans
 */
public class IORequest {

    private final IoDirections _type;
    private final String _dcache;
    private final String _store;
    private final String _netUnitName;
    private final String _protocolUnitName;
    private final String _linkGroupName;

    public IORequest(IoDirections type, String store, String dcache,
            String netUnitName, String protocolUnitName, String linkGroupName) {
        _type = type;
        _store = store;
        _dcache = dcache;
        _netUnitName = netUnitName;
        _protocolUnitName = protocolUnitName;
        _linkGroupName = linkGroupName;
    }

    public String getLinkGroupName() {
        return _linkGroupName;
    }

    public String getNetUnitName() {
        return _netUnitName;
    }

    public String getProtocolUnitName() {
        return _protocolUnitName;
    }

    public String getStore() {
        return _store;
    }

    public String getDcache() {
        return _dcache;
    }

    public IoDirections getType() {
        return _type;
    }
}
