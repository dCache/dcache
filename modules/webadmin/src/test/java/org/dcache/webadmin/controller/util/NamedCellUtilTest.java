package org.dcache.webadmin.controller.util;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author jans
 */
public class NamedCellUtilTest {

    private static final String GPLAZMA_CELL = "gPlazma";
    private static final String EMPTY_DOMAIN = "";
    private static Map<String, List<String>> _domainsMap;

    @BeforeClass
    public static void setUpClass()
    {
        _domainsMap = XMLDataGathererHelper.getDomainsMap();
    }

    @Test
    public void testFindWithEmptyDomainsMap() {
        Map<String, List<String>> emptyMap = new HashMap<>();
        String domain = NamedCellUtil.findDomainOfUniqueCell(emptyMap, GPLAZMA_CELL);
        assertEquals(EMPTY_DOMAIN, domain);
    }

    @Test
    public void testSuccessfulFind() {
        String domain = NamedCellUtil.findDomainOfUniqueCell(_domainsMap,
                XMLDataGathererHelper.POOL1_NAME);
        assertEquals(XMLDataGathererHelper.POOL1_DOMAIN, domain);
    }
}
