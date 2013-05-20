package org.dcache.webadmin.view.pages.poollist;

import org.apache.wicket.util.tester.FormTester;
import org.apache.wicket.util.tester.WicketTester;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import org.dcache.webadmin.controller.exceptions.PoolSpaceServiceException;
import org.dcache.webadmin.controller.impl.StandardPoolSpaceService;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;

import static org.junit.Assert.*;

/**
 * Unittest to test the whole page.
 * @author jans
 */
public class PoolListTest {

    public static final int DISABLED_RDONLY_CHOICE = 2;
    public static final int ENABLED_CHOICE = 0;
    public static final String DROPDOWN_ID = "mode";
    public static final String POOLUSAGE_FORM_ID = "poolUsageForm";
    public static final String HOMELINK_ID = "navigationPanel:linkList:0:link";
    public static final String LISTVIEW_ID = "poolListPanel:poolPanelListview";
    public static final String LISTVIEW_ROW_CHECKBOX_ID =
            "PoolPanel.checkboxRow:PoolPanel.selected";
    private StandardPoolSpaceService _poolSpaceService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _poolSpaceService = new StandardPoolSpaceService(daoFactory);
        authenticatedWebApp.setPoolSpaceService(_poolSpaceService);
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(PoolList.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(PoolList.class);
    }

    @Test
    public void testPoolModeChangeDisableAll() throws PoolSpaceServiceException {
        FormTester formTester = _tester.newFormTester(POOLUSAGE_FORM_ID);
        formTester.select(DROPDOWN_ID, DISABLED_RDONLY_CHOICE);
        selectAll(formTester);
        formTester.submit();
        List<PoolSpaceBean> poolsAfterChange = _poolSpaceService.getPoolBeans();
        for (PoolSpaceBean pool : poolsAfterChange) {
            assertFalse(pool.isEnabled());
        }
    }

    @Test
    public void testPoolModeChangeEnableAll() throws PoolSpaceServiceException {
        FormTester formTester = _tester.newFormTester(POOLUSAGE_FORM_ID);
        formTester.select(DROPDOWN_ID, ENABLED_CHOICE);
        selectAll(formTester);
        formTester.submit();
        List<PoolSpaceBean> poolsAfterChange = _poolSpaceService.getPoolBeans();
        for (PoolSpaceBean pool : poolsAfterChange) {
            assertTrue(pool.isEnabled());
        }
    }

    @Test
    public void testPoolModeChangeSingle() throws PoolSpaceServiceException {
        int rowToChange = 1;
        List<PoolSpaceBean> poolsBeforeChange = _poolSpaceService.getPoolBeans();
        FormTester formTester = _tester.newFormTester(POOLUSAGE_FORM_ID);
        formTester.select(DROPDOWN_ID, ENABLED_CHOICE);
        formTester.setValue(LISTVIEW_ID + ":" + rowToChange + ":" +
                LISTVIEW_ROW_CHECKBOX_ID, true);
        formTester.submit();
        List<PoolSpaceBean> poolsAfterChange = _poolSpaceService.getPoolBeans();
        assertEquals(poolsBeforeChange.get(0).isEnabled(), poolsAfterChange.get(0).isEnabled());
        assertNotSame(poolsBeforeChange.get(rowToChange).isEnabled(),
                poolsAfterChange.get(rowToChange).isEnabled());
    }

    @Test
    public void testHomeLink() {
        _tester.clickLink(HOMELINK_ID);
        _tester.assertRenderedPage(_tester.getApplication().getHomePage());
    }

    private void selectAll(FormTester formTester) {
        formTester.setValue(LISTVIEW_ID + ":0:" + LISTVIEW_ROW_CHECKBOX_ID, true);
        formTester.setValue(LISTVIEW_ID + ":1:" + LISTVIEW_ROW_CHECKBOX_ID, true);
    }

    @Test
    public void testNoRedirect() {
        WebAdminInterface notAuthenticatedWebApp =
                ApplicationFactoryHelper.createNotSignedInApplication();
        notAuthenticatedWebApp.setPoolSpaceService(_poolSpaceService);
        WicketTester redirectTester = new WicketTester(notAuthenticatedWebApp);
        redirectTester.startPage(PoolList.class);
        redirectTester.assertRenderedPage(PoolList.class);
    }
}
