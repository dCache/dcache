package org.dcache.webadmin.controller;

import java.util.List;

import diskCacheV111.pools.PoolV2Mode;

import org.dcache.webadmin.controller.exceptions.PoolSpaceServiceException;
import org.dcache.webadmin.view.beans.PoolSpaceBean;

/**
 * Services for the view to invoke
 * @author jans
 */
public interface PoolSpaceService {

    List<PoolSpaceBean> getPoolBeans() throws PoolSpaceServiceException;

    void changePoolMode(List<PoolSpaceBean> pools, PoolV2Mode poolMode,
                        String userName) throws PoolSpaceServiceException;
}
