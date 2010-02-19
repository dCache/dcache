package org.dcache.webadmin.controller.impl;

import java.util.List;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;
import org.dcache.webadmin.view.beans.PoolBean;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class PoolBeanServiceImplTest {

    private static DAOFactory DAOFactory = new DAOFactoryImplHelper();
    private PoolBeanServiceImpl poolBeanService = null;

    @Before
    public void setUp() {
        poolBeanService = new PoolBeanServiceImpl(DAOFactory);
    }

    @Test
    public void testGetPoolBeans() throws PoolBeanServiceException {
        List<PoolBean> poolBeans = poolBeanService.getPoolBeans();
        List<PoolBean> expectedPoolBeans = XMLDataGathererHelper.createExpectedPoolBeans();
        assertEquals("pools not returned as expected", expectedPoolBeans, poolBeans);
    }
}
