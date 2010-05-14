package org.dcache.webadmin.controller.impl;

import diskCacheV111.pools.PoolV2Mode;
import java.util.List;
import java.util.Set;
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

    private DAOFactory _daoFactory;
    private PoolBeanServiceImpl _poolBeanService;
    private List<PoolBean> _expectedPoolBeans;

    @Before
    public void setUp() {
        _daoFactory = new DAOFactoryImplHelper();
        _poolBeanService = new PoolBeanServiceImpl(_daoFactory);
        _expectedPoolBeans = XMLDataGathererHelper.createExpectedPoolBeans();
    }

    @Test
    public void testGetPoolBeans() throws PoolBeanServiceException {
        List<PoolBean> poolBeans = _poolBeanService.getPoolBeans();
        assertEquals("pools not returned as expected", _expectedPoolBeans, poolBeans);
    }

    @Test
    public void testChangePoolModeAllDisabled() throws PoolBeanServiceException {
        PoolsDAOImplHelper poolsDAO = (PoolsDAOImplHelper) _daoFactory.getPoolsDAO();
        setSelectionOfAllPoolBeans(true);
        _poolBeanService.changePoolMode(_expectedPoolBeans,
                new PoolV2Mode(PoolV2Mode.DISABLED_STRICT), "testuser");
        for (Pool pool : poolsDAO.getPools()) {
            assertFalse(pool.isEnabled());
        }
    }

    @Test
    public void testChangePoolModeAllEnabled() throws PoolBeanServiceException {
        PoolsDAOImplHelper poolsDAO = (PoolsDAOImplHelper) _daoFactory.getPoolsDAO();
        setSelectionOfAllPoolBeans(true);
        _poolBeanService.changePoolMode(_expectedPoolBeans,
                new PoolV2Mode(PoolV2Mode.ENABLED), "testuser");
        for (Pool pool : poolsDAO.getPools()) {
            assertTrue(pool.isEnabled());
        }
    }

    @Test
    public void testChangePoolModeNoneSelected() throws PoolBeanServiceException {
        PoolsDAOImplHelper poolsDAO = (PoolsDAOImplHelper) _daoFactory.getPoolsDAO();
        setSelectionOfAllPoolBeans(false);
        Set<Pool> poolsBeforeChange = poolsDAO.getPools();
        _poolBeanService.changePoolMode(_expectedPoolBeans,
                new PoolV2Mode(PoolV2Mode.ENABLED), "testuser");
        assertEquals(poolsBeforeChange, poolsDAO.getPools());
    }

    private void setSelectionOfAllPoolBeans(boolean selected) {
        for (PoolBean poolBean : _expectedPoolBeans) {
            poolBean.setSelected(selected);
        }
    }
}
