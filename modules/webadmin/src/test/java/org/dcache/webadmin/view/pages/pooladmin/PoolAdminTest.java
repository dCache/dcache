package org.dcache.webadmin.view.pages.pooladmin;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.controller.impl.StandardPoolAdminService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;
import org.dcache.webadmin.view.pages.login.LogIn;

/**
 * Unittest to test the whole page.
 * @author jans
 */
public class PoolAdminTest {

    private StandardPoolAdminService _poolAdminService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _poolAdminService = new StandardPoolAdminService(daoFactory);
        authenticatedWebApp.setPoolAdminService(_poolAdminService);
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(PoolAdmin.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(PoolAdmin.class);
    }

    @Test
    public void testLogInRedirect() {
        WicketTester redirectTester = new WicketTester(
                ApplicationFactoryHelper.createNotSignedInApplication());
        redirectTester.startPage(PoolAdmin.class);
        redirectTester.assertRenderedPage(LogIn.class);
    }
}
