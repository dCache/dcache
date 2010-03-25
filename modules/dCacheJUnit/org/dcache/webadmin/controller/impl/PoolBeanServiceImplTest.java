package org.dcache.webadmin.controller.impl;

import diskCacheV111.pools.PoolV2Mode;
import java.util.List;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.model.dataaccess.impl.PoolsDAOImplHelper;
import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;
import org.dcache.webadmin.view.beans.PoolBean;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class PoolBeanServiceImplTest {

    private static final DAOFactory DAO_FACTORY = new DAOFactoryImplHelper();
    private PoolBeanServiceImpl _poolBeanService = null;

    @Before
    public void setUp() {
        _poolBeanService = new PoolBeanServiceImpl(DAO_FACTORY);
    }

    @Test
    public void testGetPoolBeans() throws PoolBeanServiceException {
        List<PoolBean> poolBeans = _poolBeanService.getPoolBeans();
        List<PoolBean> expectedPoolBeans = XMLDataGathererHelper.createExpectedPoolBeans();
        assertEquals("pools not returned as expected", expectedPoolBeans, poolBeans);
    }

    @Test
    public void testChangePoolMode() throws PoolBeanServiceException {
        PoolsDAOImplHelper poolsDAO = (PoolsDAOImplHelper) DAO_FACTORY.getPoolsDAO();

        List<PoolBean> expectedPoolBeans = XMLDataGathererHelper.createExpectedPoolBeans();
        // select all pools to be disabled
        for (PoolBean poolBean : expectedPoolBeans) {
            poolBean.setSelected(true);
        }
        _poolBeanService.changePoolMode(expectedPoolBeans,
                new PoolV2Mode(PoolV2Mode.DISABLED), "testuser");

        for (Pool pool : poolsDAO.getPools()) {
            assertFalse(pool.isEnabled());
        }
    }
}
