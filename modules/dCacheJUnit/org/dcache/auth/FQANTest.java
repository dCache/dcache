package org.dcache.auth;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author jans
 */
public class FQANTest {

    public FQANTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testEquals() {
        FQAN fqan1 = new FQAN("/Desy.org/Role=Role1");
        FQAN fqan2 = new FQAN("/Desy.org/Role=Role1");
        assertTrue("FQAN not Equal, but should be", fqan1.equals(fqan2));
        assertTrue("FQAN not Equal, but should be", fqan2.equals(fqan1));
    }

    @Test
    public void testNotEquals() {
        FQAN fqan1 = new FQAN("/Desy.org/Role=Role1");
        FQAN fqan2 = new FQAN("/Desy.org/Role=Role2");
        assertFalse("FQAN Equal, but should not be", fqan1.equals(fqan2));
        assertFalse("FQAN Equal, but should not be", fqan2.equals(fqan1));
    }
}
