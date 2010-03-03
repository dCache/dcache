package org.dcache.webadmin.view.pages.dcacheservices;

import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

public class DCacheServicesTest {

    WicketTester _tester;

    @Before
    public void setUp() {
        _tester = new WicketTester();
    }

    @Test
    public void testBasicRender() {
        _tester.startPage(DCacheServices.class);
        _tester.assertRenderedPage(DCacheServices.class);
    }
}
