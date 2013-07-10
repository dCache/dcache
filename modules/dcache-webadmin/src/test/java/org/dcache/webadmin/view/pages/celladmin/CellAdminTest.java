package org.dcache.webadmin.view.pages.celladmin;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.impl.StandardCellAdminService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;
import org.dcache.webadmin.view.pages.login.LogIn;

/**
 *
 * @author jans
 */
public class CellAdminTest {

    private CellAdminService _cellAdminService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _cellAdminService = new StandardCellAdminService(daoFactory);
        authenticatedWebApp.setCellAdminService(_cellAdminService);
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(CellAdmin.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(CellAdmin.class);
    }

    @Test
    public void testLogInRedirect() {
        WicketTester redirectTester = new WicketTester(
                ApplicationFactoryHelper.createNotSignedInApplication());
        redirectTester.startPage(CellAdmin.class);
        redirectTester.assertRenderedPage(LogIn.class);
    }
}
