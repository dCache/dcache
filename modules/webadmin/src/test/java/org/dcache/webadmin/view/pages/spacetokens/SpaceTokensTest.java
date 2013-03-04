package org.dcache.webadmin.view.pages.spacetokens;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.controller.impl.StandardLinkGroupsService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

/**
 *
 * @author jans
 */
public class SpaceTokensTest {

    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        authenticatedWebApp.setLinkGroupsService(
                new StandardLinkGroupsService(daoFactory));
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(SpaceTokens.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(SpaceTokens.class);
    }
}
