package org.dcache.webadmin.view.pages.poolqueues;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.controller.impl.StandardPoolQueuesService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

/**
 * Unittest to test the whole page.
 * @author jans
 */
public class PoolQueuesTest {

    private StandardPoolQueuesService _poolQueuesService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _poolQueuesService = new StandardPoolQueuesService(daoFactory);
        authenticatedWebApp.setPoolQueuesService(_poolQueuesService);
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(PoolQueues.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(PoolQueues.class);
    }

    @Test
    public void testNoRedirect() {
        WebAdminInterface notAuthenticatedWebApp =
                ApplicationFactoryHelper.createNotSignedInApplication();
        notAuthenticatedWebApp.setPoolQueuesService(_poolQueuesService);
        WicketTester redirectTester = new WicketTester(notAuthenticatedWebApp);
        redirectTester.startPage(PoolQueues.class);
        redirectTester.assertRenderedPage(PoolQueues.class);
    }
}
