package org.dcache.webadmin.model.dataaccess.communication.impl;

import dmg.cells.nucleus.CellNucleus;

/**
 * Holds the CellNucleus to make the Domaincontext available for webadmin
 * dataaccessobjects
 * @author jans
 */
public class CellDomainContextHolder {

    private CellNucleus _nucleus;

    public CellDomainContextHolder(CellNucleus nucleus) {
        if (nucleus == null) {
            throw new IllegalArgumentException("no nucleus provided");
        }
        _nucleus = nucleus;
    }

    public Object getDomainContext(String context) {
        return _nucleus.getDomainContext(context);
    }
}
