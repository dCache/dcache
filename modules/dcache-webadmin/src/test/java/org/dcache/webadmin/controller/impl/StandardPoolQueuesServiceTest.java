package org.dcache.webadmin.controller.impl;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolQueuesServiceException;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.beans.PoolQueueBean;

import static org.junit.Assert.assertNotNull;

/**
 *
 * @author jans
 */
public class StandardPoolQueuesServiceTest {

    private DAOFactory _daoFactory;
    private StandardPoolQueuesService _poolQueuesService;

    @Before
    public void setUp() {
        _daoFactory = new DAOFactoryImplHelper();
        _poolQueuesService = new StandardPoolQueuesService(_daoFactory);
    }

    @Test
    public void testGetPoolBeans() throws PoolQueuesServiceException {
        List<PoolQueueBean> poolQueues = _poolQueuesService.getPoolQueues();
        assertNotNull(poolQueues);
    }
}
