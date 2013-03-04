package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolSelectionSetupServiceException;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.DCacheEntityContainerBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.IORequest;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.MatchBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PartitionsBean;

/**
 *
 * @author jans
 */
public interface PoolSelectionSetupService {

    public List<PartitionsBean> getPartitions() throws PoolSelectionSetupServiceException;

    public List<MatchBean> getMatchForIORequest(IORequest request) throws PoolSelectionSetupServiceException;

    public DCacheEntityContainerBean getEntityContainer() throws PoolSelectionSetupServiceException;
}
