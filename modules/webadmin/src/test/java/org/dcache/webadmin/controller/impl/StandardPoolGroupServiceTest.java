package org.dcache.webadmin.controller.impl;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolGroupServiceException;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.model.dataaccess.impl.PoolsDAOImplHelper;
import org.dcache.webadmin.view.beans.PoolGroupBean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 *
 * @author jans
 */
public class StandardPoolGroupServiceTest {

    private DAOFactory _daoFactory;
    private StandardPoolGroupService _poolGroupService;

    @Before
    public void setUp() {
        _daoFactory = new DAOFactoryImplHelper();
        _poolGroupService = new StandardPoolGroupService(_daoFactory);
    }

    @Test
    public void testGetPoolGroups() throws PoolGroupServiceException {
        List<PoolGroupBean> poolGroups = _poolGroupService.getPoolGroups();
        assertNotNull(poolGroups);
    }

    @Test(expected = PoolGroupServiceException.class)
    public void testGetPoolGroupsWithDAOException() throws PoolGroupServiceException {
        PoolsDAOImplHelper dao = (PoolsDAOImplHelper) _daoFactory.getPoolsDAO();
        dao.setAlwaysThrowsDaoException(true);
        List<PoolGroupBean> poolGroups = _poolGroupService.getPoolGroups();
        fail("no exception thrown");
    }
}
