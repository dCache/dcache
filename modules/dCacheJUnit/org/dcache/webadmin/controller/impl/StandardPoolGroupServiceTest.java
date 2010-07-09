package org.dcache.webadmin.controller.impl;

import java.util.List;
import org.dcache.webadmin.controller.exceptions.PoolGroupServiceException;
import org.junit.Before;
import org.junit.Test;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.model.dataaccess.impl.PoolGroupDAOHelper;
import org.dcache.webadmin.view.beans.PoolGroupBean;
import static org.junit.Assert.*;

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
        PoolGroupDAOHelper dao = (PoolGroupDAOHelper) _daoFactory.getPoolGroupDAO();
        dao.setAlwaysThrowsDaoException(true);
        List<PoolGroupBean> poolGroups = _poolGroupService.getPoolGroups();
        fail("no exception thrown");
    }
}
