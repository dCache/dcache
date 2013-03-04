package org.dcache.webadmin.view.pages.poolgroupview;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.controller.impl.StandardPoolGroupService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

/**
 * Unittest to test the whole page.
 * @author jans
 */
public class PoolGroupViewTest {

    public static final String POOLGROUPS_FORM_ID = "poolGroupsForm";
    public static final String CELLVIEW_LINK_ID = "poolGroupsForm:miniNavigationFragment:cellViewLink";
    public static final String SPACEUSAGE_LINK_ID = "poolGroupsForm:miniNavigationFragment:spaceUsageLink";
    public static final String MOVERVIEW_LINK_ID = "poolGroupsForm:miniNavigationFragment:moverViewLink";
    private StandardPoolGroupService _poolGroupService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _poolGroupService = new StandardPoolGroupService(daoFactory);
        authenticatedWebApp.setPoolGroupService(_poolGroupService);
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(PoolGroupView.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(PoolGroupView.class);
    }

    @Test
    public void testClickingLinksWithoutGroupChosen() {
        _tester.clickLink(CELLVIEW_LINK_ID);
        _tester.clickLink(SPACEUSAGE_LINK_ID);
        _tester.clickLink(MOVERVIEW_LINK_ID);
    }

    @Test
    public void testNoRedirect() {
        WebAdminInterface notAuthenticatedWebApp =
                ApplicationFactoryHelper.createNotSignedInApplication();
        notAuthenticatedWebApp.setPoolGroupService(_poolGroupService);
        WicketTester redirectTester = new WicketTester(notAuthenticatedWebApp);
        redirectTester.startPage(PoolGroupView.class);
        redirectTester.assertRenderedPage(PoolGroupView.class);
    }
}
