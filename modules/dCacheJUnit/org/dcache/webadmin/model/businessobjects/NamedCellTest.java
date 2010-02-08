package org.dcache.webadmin.model.businessobjects;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author jans
 */
public class NamedCellTest {

    private final String TEST_DOMAIN = "testDomain";
    private final String TEST_NAME = "testName";
    private NamedCell namedCell = null;

    @Before
    public void setUp() {
        namedCell = new NamedCell();
    }

    @Test
    public void testAttributes() {
        namedCell.setCellName(TEST_NAME);
        namedCell.setDomainName(TEST_DOMAIN);
        assertEquals("set-Method failed", TEST_DOMAIN, namedCell.getDomainName());
        assertEquals("set-Method failed", TEST_NAME, namedCell.getCellName());
    }

    @Test
    public void testHashCode() {
        NamedCell otherNamedCell = new NamedCell();
        assertTrue(namedCell.hashCode() == otherNamedCell.hashCode());

        namedCell.setCellName(TEST_NAME);
        namedCell.setDomainName(TEST_DOMAIN);
        assertFalse(namedCell.hashCode() == otherNamedCell.hashCode());

        otherNamedCell.setCellName(TEST_NAME);
        otherNamedCell.setDomainName(TEST_NAME);
        assertFalse(namedCell.hashCode() == otherNamedCell.hashCode());

        otherNamedCell.setDomainName(TEST_DOMAIN);
        assertTrue(namedCell.hashCode() == otherNamedCell.hashCode());
    }

    @Test
    public void testEquals() {
        NamedCell otherNamedCell = new NamedCell();
        assertTrue(namedCell.equals(otherNamedCell));

        namedCell.setCellName(TEST_NAME);
        namedCell.setDomainName(TEST_DOMAIN);
        assertFalse(namedCell.equals(otherNamedCell));

        otherNamedCell.setCellName(TEST_NAME);
        otherNamedCell.setDomainName(TEST_NAME);
        assertFalse(namedCell.equals(otherNamedCell));

        otherNamedCell.setDomainName(TEST_DOMAIN);
        assertTrue(namedCell.equals(otherNamedCell));
    }
}
