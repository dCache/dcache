package org.dcache.webadmin.view.pages.login;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

public class LogInTest {

    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createStandardApplication();
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(LogIn.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(LogIn.class);
    }
}
