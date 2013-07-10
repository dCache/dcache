package org.dcache.webadmin.view.pages.poolselectionsetup;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.controller.impl.StandardPoolSelectionSetupService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

/**
 * Unittest to test the whole page.
 * @author jans
 */
public class PoolSelectionSetupTest {

    private StandardPoolSelectionSetupService _selectionSetupService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _selectionSetupService = new StandardPoolSelectionSetupService(daoFactory);
        authenticatedWebApp.setPoolSelectionSetupService(_selectionSetupService);
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(PoolSelectionSetup.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(PoolSelectionSetup.class);
    }
}
