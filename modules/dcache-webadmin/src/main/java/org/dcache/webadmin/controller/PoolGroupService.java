package org.dcache.webadmin.controller;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolGroupServiceException;
import org.dcache.webadmin.view.beans.PoolGroupBean;

/**
 * Service that delivers PoolGroup related methods
 * @author jans
 */
public interface PoolGroupService {

    List<PoolGroupBean> getPoolGroups() throws PoolGroupServiceException;
}
