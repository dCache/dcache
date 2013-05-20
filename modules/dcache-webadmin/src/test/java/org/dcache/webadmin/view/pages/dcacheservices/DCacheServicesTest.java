package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

public class DCacheServicesTest {

    WicketTester _tester;

    @Before
    public void setUp() {
        _tester = new WicketTester(ApplicationFactoryHelper.createSignedInAsAdminApplication());
    }

    @Test
    public void testBasicRender() {
        _tester.startPage(DCacheServices.class);
        _tester.assertRenderedPage(DCacheServices.class);
    }

    @Test
    public void testNoRedirect() {
        WicketTester redirectTester = new WicketTester(
                ApplicationFactoryHelper.createNotSignedInApplication());
        redirectTester.startPage(DCacheServices.class);
        redirectTester.assertRenderedPage(DCacheServices.class);
    }
}
