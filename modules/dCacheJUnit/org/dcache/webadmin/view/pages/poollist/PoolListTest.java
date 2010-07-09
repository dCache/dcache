package org.dcache.webadmin.view.pages.poollist;

import java.util.List;
import org.apache.wicket.util.tester.FormTester;
import org.apache.wicket.util.tester.WicketTester;
import org.dcache.webadmin.controller.exceptions.PoolBeanServiceException;
import org.dcache.webadmin.controller.impl.PoolBeanServiceImpl;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.impl.DAOFactoryImplHelper;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.PoolBean;
import org.dcache.webadmin.view.pages.ApplicationFactoryHelper;
import org.dcache.webadmin.view.pages.login.LogIn;
import org.junit.Before;
import org.junit.Test;
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
    private PoolBeanServiceImpl _poolBeanService;
    private WicketTester _tester;

    @Before
    public void setUp() {
        WebAdminInterface authenticatedWebApp =
                ApplicationFactoryHelper.createSignedInAsAdminApplication();
        DAOFactory daoFactory = new DAOFactoryImplHelper();
        _poolBeanService = new PoolBeanServiceImpl(daoFactory);
        authenticatedWebApp.setPoolBeanService(_poolBeanService);
        _tester = new WicketTester(authenticatedWebApp);
        _tester.startPage(PoolList.class);
    }

    @Test
    public void testBasicRender() {
        _tester.assertRenderedPage(PoolList.class);
    }

    @Test
    public void testPoolModeChangeDisableAll() throws PoolBeanServiceException {
        FormTester formTester = _tester.newFormTester(POOLUSAGE_FORM_ID);
        formTester.select(DROPDOWN_ID, DISABLED_RDONLY_CHOICE);
        selectAll(formTester);
        formTester.submit();
        List<PoolBean> poolsAfterChange = _poolBeanService.getPoolBeans();
        for (PoolBean pool : poolsAfterChange) {
            assertFalse(pool.isEnabled());
        }
    }

    @Test
    public void testPoolModeChangeEnableAll() throws PoolBeanServiceException {
        FormTester formTester = _tester.newFormTester(POOLUSAGE_FORM_ID);
        formTester.select(DROPDOWN_ID, ENABLED_CHOICE);
        selectAll(formTester);
        formTester.submit();
        List<PoolBean> poolsAfterChange = _poolBeanService.getPoolBeans();
        for (PoolBean pool : poolsAfterChange) {
            assertTrue(pool.isEnabled());
        }
    }

    @Test
    public void testPoolModeChangeSingle() throws PoolBeanServiceException {
        int rowToChange = 1;
        List<PoolBean> poolsBeforeChange = _poolBeanService.getPoolBeans();
        FormTester formTester = _tester.newFormTester(POOLUSAGE_FORM_ID);
        formTester.select(DROPDOWN_ID, ENABLED_CHOICE);
        formTester.setValue(LISTVIEW_ID + ":" + rowToChange + ":" +
                LISTVIEW_ROW_CHECKBOX_ID, true);
        formTester.submit();
        List<PoolBean> poolsAfterChange = _poolBeanService.getPoolBeans();
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
    public void testLogInRedirect() {
        WicketTester redirectTester = new WicketTester(
                ApplicationFactoryHelper.createNotSignedInApplication());
        redirectTester.startPage(PoolList.class);
        redirectTester.assertRenderedPage(LogIn.class);
    }
}
