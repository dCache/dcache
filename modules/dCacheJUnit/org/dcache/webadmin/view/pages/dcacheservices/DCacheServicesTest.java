package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.util.tester.WicketTester;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;
import org.dcache.webadmin.view.pages.login.LogIn;
import org.junit.Before;
import org.junit.Test;

public class DCacheServicesTest {

    WicketTester _tester;

    @Before
    public void setUp() {
        _tester = new WicketTester(ApplicationFactoryHelper.createSignedInApplication());
    }

    @Test
    public void testBasicRender() {
        _tester.startPage(DCacheServices.class);
        _tester.assertRenderedPage(DCacheServices.class);
    }

    @Test
    public void testLogInRedirect() {
        WicketTester redirectTester = new WicketTester(
                ApplicationFactoryHelper.createNotSignedInApplication());
        redirectTester.startPage(DCacheServices.class);
        redirectTester.assertRenderedPage(LogIn.class);
    }
}
