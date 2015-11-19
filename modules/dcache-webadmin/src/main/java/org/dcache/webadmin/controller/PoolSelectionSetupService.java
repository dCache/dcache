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

    List<PartitionsBean> getPartitions() throws PoolSelectionSetupServiceException;

    List<MatchBean> getMatchForIORequest(IORequest request) throws PoolSelectionSetupServiceException;

    DCacheEntityContainerBean getEntityContainer() throws PoolSelectionSetupServiceException;
}
