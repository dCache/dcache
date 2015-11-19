package org.dcache.webadmin.controller;

import java.util.Collection;
import java.util.List;

import org.dcache.webadmin.controller.exceptions.ActiveTransfersServiceException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

public interface ActiveTransfersService
{
    List<ActiveTransfersBean> getTransfers();

    void kill(Collection<ActiveTransfersBean.Key> keys)
            throws ActiveTransfersServiceException;
}
