package org.dcache.webadmin.view.pages.poollist;

import org.apache.wicket.util.tester.WicketTester;
import org.dcache.webadmin.controller.impl.PoolBeanServiceImpl;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.junit.Before;
import org.junit.Test;

/**
 * Unittest to test the whole page.
 * @author jans
 */
public class PoolListTest {

    private static DAOFactory _DAOFactory = new DAOFactoryImplHelper();
    private PoolBeanServiceImpl _poolBeanService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp = new WebAdminInterface();
        _poolBeanService = new PoolBeanServiceImpl(_DAOFactory);
        authenticatedWebApp.setPoolBeanService(_poolBeanService);
        _tester = new WicketTester(authenticatedWebApp);
    }

    @Test
    public void testBasicRender() {
        _tester.startPage(PoolList.class);
        _tester.assertRenderedPage(PoolList.class);
    }
}
