package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolAdminServiceException;
import org.dcache.webadmin.view.beans.PoolAdminBean;
import org.dcache.webadmin.view.beans.PoolCommandBean;
import org.dcache.webadmin.view.util.SelectableWrapper;

/**
 *
 * @author jans
 */
public interface PoolAdminService {

    List<PoolAdminBean> getPoolGroups() throws PoolAdminServiceException;

    void sendCommand(
            List<SelectableWrapper<PoolCommandBean>> pools, String command)
            throws PoolAdminServiceException;
}
