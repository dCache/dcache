package org.dcache.webadmin.view.pages.poolqueues;

import org.apache.wicket.util.tester.WicketTester;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author jans
 */
public class PoolQueuesTest {

    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(PoolQueues.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(PoolQueues.class);
    }
}
