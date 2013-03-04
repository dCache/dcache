package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.ActiveTransfersServiceException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;
import org.dcache.webadmin.view.util.SelectableWrapper;

/**
 *
 * @author jans
 */
public interface ActiveTransfersService {

    public List<SelectableWrapper<ActiveTransfersBean>> getActiveTransferBeans()
            throws ActiveTransfersServiceException;

    public void killTransfers(List<SelectableWrapper<ActiveTransfersBean>> transfers)
            throws ActiveTransfersServiceException;
}
