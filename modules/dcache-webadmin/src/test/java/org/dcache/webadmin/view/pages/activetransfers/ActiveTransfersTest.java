package org.dcache.webadmin.view.pages.activetransfers;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.impl.StandardActiveTransfersService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

/**
 * Unittest to test the whole page.
 * @author jans
 */
public class ActiveTransfersTest {

    public static final String HOMELINK_ID = "navigationPanel:linkList:0:link";
    private ActiveTransfersService _activeTransfersService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _activeTransfersService = new StandardActiveTransfersService(daoFactory);
        authenticatedWebApp.setActiveTransfersService(_activeTransfersService);
        _tester = new WicketTester(authenticatedWebApp);
        ActiveTransfers.autorefresh = false;
        _tester.startPage(ActiveTransfers.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(ActiveTransfers.class);
    }

    @Test
    public void testHomeLink() {
        _tester.clickLink(HOMELINK_ID);
        _tester.assertRenderedPage(_tester.getApplication().getHomePage());
    }
}
