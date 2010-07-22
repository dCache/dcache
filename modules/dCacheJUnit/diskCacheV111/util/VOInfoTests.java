package diskCacheV111.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class VOInfoTests {

    public static final String VO_NAME = "atlas";
    public static final String OTHER_VO_NAME = "cms";

    public static final String FQAN_NO_ROLE = "/" + VO_NAME;
    public static final String OTHER_FQAN_NO_ROLE = "/" + OTHER_VO_NAME;

    public static final String FQAN_WITH_WILDCARD_VO_AND_NO_ROLE = "/*";

    public static final String ROLE_NAME = "production";
    public static final String OTHER_ROLE_NAME = "observer";
    public static final String FQAN_WITH_ROLE = "/" + VO_NAME + "/Role=" + ROLE_NAME;
    public static final String FQAN_WITH_WILDCARD_ROLE = "/" + VO_NAME + "/Role=*";

    public static final String FQAN_WITH_WILDCARD_VO_AND_WILDCARD_ROLE = "/*/Role=*";

    /*
     * A set of tests for FQAN globbing patterns
     */

    @Test
    public void testMatchForFqanStringWithGroupNoRole() {
        VOInfo test = new VOInfo(FQAN_NO_ROLE);

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertFalse( test.match( FQAN_NO_ROLE, ROLE_NAME));

        assertFalse( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForFqanStringWithGroupAndRole() {
        VOInfo test = new VOInfo(FQAN_WITH_ROLE);

        assertFalse( test.match( FQAN_NO_ROLE, ""));
        assertTrue( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertFalse( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertFalse( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForFqanStringWithGroupAndWildcardRole() {
        VOInfo test = new VOInfo(FQAN_WITH_WILDCARD_ROLE);

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertTrue( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertTrue( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertFalse( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForFqanStringWithWildcardGroupAndNoRole() {
        VOInfo test = new VOInfo(FQAN_WITH_WILDCARD_VO_AND_NO_ROLE);

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertFalse( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertFalse( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertTrue( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForFqanStringWithWildcardGroupAndWildcardRole() {
        VOInfo test = new VOInfo(FQAN_WITH_WILDCARD_VO_AND_WILDCARD_ROLE);

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertTrue( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertTrue( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertTrue( test.match( OTHER_FQAN_NO_ROLE, ""));
        assertTrue( test.match( OTHER_FQAN_NO_ROLE, OTHER_ROLE_NAME));
    }


    /*
     * A set of tests equivalent to the FQAN-globbing tests but
     * using the two-string constructor
     */

    @Test
    public void testMatchForTwoStringsWithGroupNoRole() {
        VOInfo test = new VOInfo(FQAN_NO_ROLE, "");

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertFalse( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertFalse( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertFalse( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForTwoStringsWithGroupAndRole() {
        VOInfo test = new VOInfo(FQAN_NO_ROLE, ROLE_NAME);

        assertFalse( test.match( FQAN_NO_ROLE, ""));
        assertTrue( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertFalse( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertFalse( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForTwoStringsWithGroupAndWildcardRole() {
        VOInfo test = new VOInfo(FQAN_NO_ROLE, "*");

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertTrue( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertTrue( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertFalse( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForTwoStringsWithWildcardGroupAndNoRole() {
        VOInfo test = new VOInfo(FQAN_WITH_WILDCARD_VO_AND_NO_ROLE, "");

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertFalse( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertFalse( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertTrue( test.match( OTHER_FQAN_NO_ROLE, ""));
    }

    @Test
    public void testMatchForTwoStringsWithWildcardGroupAndWildcardRole() {
        VOInfo test = new VOInfo(FQAN_WITH_WILDCARD_VO_AND_NO_ROLE, "*");

        assertTrue( test.match( FQAN_NO_ROLE, ""));
        assertTrue( test.match( FQAN_NO_ROLE, ROLE_NAME));
        assertTrue( test.match( FQAN_NO_ROLE, OTHER_ROLE_NAME));

        assertTrue( test.match( OTHER_FQAN_NO_ROLE, ""));
        assertTrue( test.match( OTHER_FQAN_NO_ROLE, OTHER_ROLE_NAME));
    }

    /*
     * Various special cases
     */

    @Test
    public void testMatchForTwoStringsWithSelectiveMatch() {
        VOInfo test = new VOInfo("gr*", "r*le");

        assertTrue( test.match( "group", "role"));
        assertTrue( test.match( "group", "ridicule"));
        assertTrue( test.match( "grudgingly", "ridicule"));
    }

    @Test
    public void testMatchForTwoStringsWithWildGroupAndWrongRole() {
        VOInfo test = new VOInfo("*", "r*ule");

        assertTrue( test.match( "group", "rule"));
        assertTrue( test.match( "group", "ridicule"));
        assertFalse( test.match( "group", "role"));
    }


    @Test
    public void testMatchForTwoStringsWithGroupAndNullRole() {
        VOInfo test = new VOInfo( VO_NAME, null);

        assertTrue( test.match( VO_NAME, null));
        assertTrue( test.match( VO_NAME, ROLE_NAME));
        assertTrue( test.match( VO_NAME, OTHER_ROLE_NAME));

        assertFalse( test.match( OTHER_VO_NAME, null));
        assertFalse( test.match( OTHER_VO_NAME, ROLE_NAME));
        assertFalse( test.match( OTHER_VO_NAME, OTHER_ROLE_NAME));
    }

}
