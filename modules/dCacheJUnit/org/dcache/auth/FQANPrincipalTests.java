package org.dcache.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class FQANPrincipalTests {

    public static final String TEST_FQAN_STRING = "/atlas";
    public static final FQAN TEST_FQAN = new FQAN( TEST_FQAN_STRING);

    public static final String OTHER_TEST_FQAN_STRING = "/cms";

    @Test
    public void testCreateFromValidString() {
        FQANPrincipal principal = new FQANPrincipal( TEST_FQAN_STRING, true);
        assertEquals( "Check principal fqan in string form", TEST_FQAN_STRING,
                principal.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromNullStringFails() {
        new FQANPrincipal( (String) null, true);
    }

    @Test
    public void testCreateFromValidFqan() {
        FQANPrincipal principal = new FQANPrincipal( TEST_FQAN, true);
        assertEquals( "Check principal has correct FQAN", TEST_FQAN,
                principal.getFqan());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromNullFqanFails() {
        new FQANPrincipal( (FQAN) null, true);
    }

    @Test
    public void testCreatePrimary() {
        FQANPrincipal principal = new FQANPrincipal( TEST_FQAN, true);
        assertTrue( "Check principal is primary", principal.isPrimary());
    }

    @Test
    public void testCreateNonPrimary() {
        FQANPrincipal principal = new FQANPrincipal( TEST_FQAN, false);
        assertFalse( "Check principal is not primary", principal.isPrimary());
    }

    @Test
    public void testEqualsForSameObject() {
        FQANPrincipal principal = new FQANPrincipal( TEST_FQAN, false);
        assertTrue( "Checking principal equals itself",
                principal.equals( principal));
    }

    @Test
    public void testNotEqualsForDifferentClass() {
        FQANPrincipal principal = new FQANPrincipal( TEST_FQAN, false);
        assertFalse( "Checking principal not equal to FQAN",
                principal.equals( TEST_FQAN));
        assertFalse( "Checking principal not equal to FQAN-string",
                principal.equals( TEST_FQAN_STRING));
    }

    @Test
    public void testEqualForEqualFqanPrimary() {
        FQANPrincipal p1 = new FQANPrincipal( TEST_FQAN, true);
        FQANPrincipal p2 = new FQANPrincipal( TEST_FQAN, true);
        assertTrue( "Checking p1 equals p2", p1.equals( p2));
        assertTrue( "Checking p2 equals p1", p2.equals( p1));
    }

    @Test
    public void testEqualForEqualFqanNonPrimary() {
        FQANPrincipal p1 = new FQANPrincipal( TEST_FQAN_STRING, false);
        FQANPrincipal p2 = new FQANPrincipal( TEST_FQAN_STRING, false);
        assertTrue( "Checking p1 equals p2", p1.equals( p2));
        assertTrue( "Checking p2 equals p1", p2.equals( p1));
    }

    @Test
    public void testNotEqualByFqan() {
        FQANPrincipal p1 = new FQANPrincipal( TEST_FQAN_STRING, false);
        FQANPrincipal p2 = new FQANPrincipal( OTHER_TEST_FQAN_STRING, false);
        assertFalse( "Checking p1 doesn't equal p2", p1.equals( p2));
        assertFalse( "Checking p2 doesn't equal p1", p2.equals( p1));
    }

    @Test
    public void testNotEqualByPrimary() {
        FQANPrincipal p1 = new FQANPrincipal( TEST_FQAN_STRING, true);
        FQANPrincipal p2 = new FQANPrincipal( TEST_FQAN_STRING, false);
        assertFalse( "Checking p1 doesn't equal p2", p1.equals( p2));
        assertFalse( "Checking p2 doesn't equal p1", p2.equals( p1));
    }

    @Test
    public void testGetFqanForFqanCreatedPrincipal() {
        FQANPrincipal principal = new FQANPrincipal( TEST_FQAN, true);
        assertEquals( "checking getFqan gets expected result", TEST_FQAN,
                principal.getFqan());
    }
}
