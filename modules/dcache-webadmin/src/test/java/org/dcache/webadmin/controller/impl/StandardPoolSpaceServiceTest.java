package org.dcache.webadmin.controller.impl;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import diskCacheV111.pools.PoolV2Mode;

import org.dcache.webadmin.controller.exceptions.PoolSpaceServiceException;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.model.dataaccess.impl.PoolsDAOImplHelper;
import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;
import org.dcache.webadmin.view.beans.PoolSpaceBean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandardPoolSpaceServiceTest {

    private DAOFactory _daoFactory;
    private StandardPoolSpaceService _poolBeanService;
    private List<PoolSpaceBean> _expectedPoolBeans;

    @Before
    public void setUp() {
        _daoFactory = new DAOFactoryImplHelper();
        _poolBeanService = new StandardPoolSpaceService(_daoFactory);
        _expectedPoolBeans = XMLDataGathererHelper.createExpectedPoolBeans();
    }

    @Test
    public void testGetPoolBeans() throws PoolSpaceServiceException {
        List<PoolSpaceBean> poolBeans = _poolBeanService.getPoolBeans();
        assertEquals("pools not returned as expected", _expectedPoolBeans, poolBeans);
    }

    @Test
    public void testChangePoolModeAllDisabled() throws PoolSpaceServiceException {
        PoolsDAOImplHelper poolsDAO = (PoolsDAOImplHelper) _daoFactory.getPoolsDAO();
        setSelectionOfAllPoolBeans(true);
        _poolBeanService.changePoolMode(_expectedPoolBeans,
                new PoolV2Mode(PoolV2Mode.DISABLED_STRICT), "testuser");
        for (Pool pool : poolsDAO.getPools()) {
            assertTrue(pool.getSelectionPool().getPoolMode().getMode() == PoolV2Mode.DISABLED_STRICT);
        }
    }

    @Test
    public void testChangePoolModeAllEnabled() throws PoolSpaceServiceException {
        PoolsDAOImplHelper poolsDAO = (PoolsDAOImplHelper) _daoFactory.getPoolsDAO();
        setSelectionOfAllPoolBeans(true);
        _poolBeanService.changePoolMode(_expectedPoolBeans,
                new PoolV2Mode(PoolV2Mode.ENABLED), "testuser");
        for (Pool pool : poolsDAO.getPools()) {
            assertTrue(pool.getSelectionPool().getPoolMode().getMode() == PoolV2Mode.ENABLED);
        }
    }

    @Test
    public void testChangePoolModeNoneSelected() throws PoolSpaceServiceException {
        PoolsDAOImplHelper poolsDAO = (PoolsDAOImplHelper) _daoFactory.getPoolsDAO();
        setSelectionOfAllPoolBeans(false);
        Set<Pool> poolsBeforeChange = poolsDAO.getPools();
        _poolBeanService.changePoolMode(_expectedPoolBeans,
                new PoolV2Mode(PoolV2Mode.ENABLED), "testuser");
        assertEquals(poolsBeforeChange, poolsDAO.getPools());
    }

    private void setSelectionOfAllPoolBeans(boolean selected) {
        for (PoolSpaceBean poolBean : _expectedPoolBeans) {
            poolBean.setSelected(selected);
        }
    }
}
