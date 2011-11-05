package org.dcache.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * This class provides test-cases for the FQAN class.
 *
 * From page 6 of
 *
 *   http://www.ogf.org/Public_Comment_Docs/Documents/2009-06/VOMSACv10-editor-1.doc.pdf
 *
 *    "This information is encoded in a Fully Qualified Attribute Name (FQAN),
 *    in the following format:
 *
 *            <group name>/Role=<role name>/Capability=<capability name>
 *
 *    This syntax means that the user holds the role <role name> in the group
 *    <group name>. If no specific role is held, the <role name> is NULL. The
 *    /Capability=<capability name> part is deprecated and will disappear in
 *    the future: conforming applications SHOULD be able to handle FQANs where
 *    it is absent and SHOULD NOT rely on its presence.
 *
 *    Future compatibility issue: It is possible that in the future a
 *    /Role=NULL component may be omitted in its entirety. The same goes for a
 *    /Capability=NULL part. Conforming applications SHOULD be prepared to
 *    handle these cases."
 *
 * @author jans
 * @author Paul Millar <paul.millar@desy.de>
 */
public class FQANTest {

    /**
     * The items that may make up a FQAN.  The VO is always
     * present, but GROUP, ROLE and CAPABILITY are optional.
     */
    public enum FqanElements {
        VO, GROUP, ROLE, CAPABILITY
    }

    /* Main set of data used to build test FQANs */
    private static final String VO_NAME = "atlas";
    private static final String GROUP_NAME = "higgs";
    private static final String ROLE_NAME = "production";
    private static final String CAPABILITY_NAME = "some-capability";

    /* Alternative non-equal data used for inequality tests */
    private static final String OTHER_VO_NAME = "cms";
    private static final String OTHER_GROUP_NAME = "LAr";
    private static final String OTHER_ROLE_NAME = "observer";
    private static final String OTHER_CAPABILITY_NAME = "other-capability";

    private static final String EMPTY_STRING = "";

    private static final FQAN[] ALL_TYPES = buildFqanArrayOfAllTypes();

    private static final String[] INVALID_FQANS = {
        null,                            // null isn't valid
        VO_NAME,                         // lack of initial '/'
        "/*",                            // illegal character of VO
        "/-" + VO_NAME,                  // illegal initial character of VO
        "/0" + VO_NAME,                  //    "       "        "      "  "
        "/A" + VO_NAME,                  //    "       "        "      "  "
        "/" + VO_NAME + "-",             // illegal final character of VO
        "/" + VO_NAME + ".",             //    "      "        "     "  "
        "/" + VO_NAME + "/",             // empty group not allowed
        "/" + VO_NAME + "/*",            // illegal character in (sub)group
        "/" + VO_NAME + "/=" + GROUP_NAME,          // illegal character in (sub)group
        "/" + VO_NAME + "/" + GROUP_NAME + "=",     // illegal character in (sub)group
        "/" + VO_NAME + "/" + GROUP_NAME + "=NULL", // illegal character in (sub)group
        "/" + VO_NAME + "/Role=",                   // empty role not allowed
        "/" + VO_NAME + "/Role=" + ROLE_NAME + " ", // illegal character in role
        "/" + VO_NAME + "/Role= " + ROLE_NAME,      // illegal character in role
        "/" + VO_NAME + "/Role=" + ROLE_NAME + ".", // illegal character in role
        "/" + VO_NAME + "/Role=." + ROLE_NAME,      // illegal character in role
        "/" + VO_NAME + "/Role=*",                  // illegal character in role
        "/" + VO_NAME + "/Capability=",             // empty capability not allowed
        "/" + VO_NAME + "/Capability=" + CAPABILITY_NAME + " ", // illegal character in capability
        "/" + VO_NAME + "/Capability= " + CAPABILITY_NAME,      // illegal character in capability
        "/" + VO_NAME + "/Capability=" + CAPABILITY_NAME + ".", // illegal character in capability
        "/" + VO_NAME + "/Capability=." + CAPABILITY_NAME,      // illegal character in capability
    };

    @Test(expected=IllegalArgumentException.class)
    public void testCreateWithNullNotAllowed() {
        new FQAN(null);
    }

    @Test
    public void testTypesEqualsIsReflexive() {
        for( FQAN fqan1 : ALL_TYPES) {
            assertEquals( fqan1, fqan1);

            FQAN fqan2 = new FQAN( fqan1.toString());
            assertEquals( fqan1, fqan2);
        }
    }

    @Test
    public void testDifferentTypesNotEqual() {
        for( int i = 0; i < ALL_TYPES.length; i++) {
            FQAN fqan1 = ALL_TYPES[i];

            for( int j = 0; j < ALL_TYPES.length; j++) {
                FQAN fqan2 = ALL_TYPES[j];

                if( i != j) {
                    assertFalse( fqan1.equals( fqan2));
                }
            }
        }
    }

    @Test
    public void testNotEqualByVO() {
        FQAN[] otherVoFqans = buildFqanArrayWithOtherVo( OTHER_VO_NAME);
        assertNoneEqual( otherVoFqans);
    }

    @Test
    public void testNotEqualByGroup() {
        FQAN[] otherGroupFqans =
                buildFqanArrayWithOtherGroup( OTHER_GROUP_NAME);
        assertNoneEqual( otherGroupFqans);
    }

    @Test
    public void testNotEqualByRole() {
        FQAN[] otherRoleFqans = buildFqanArrayWithOtherRole( OTHER_ROLE_NAME);
        assertNoneEqual( otherRoleFqans);
    }

    @Test
    public void testNotEqualByCapability() {
        FQAN[] otherCapabilityFqans =
                buildFqanArrayWithOtherCapability( OTHER_CAPABILITY_NAME);
        assertNoneEqual( otherCapabilityFqans);
    }

    @Test
    public void testGetGroupForFqansWithGroup() {
        FQAN[] fqansWithGroup = buildFqanArrayWith( FqanElements.GROUP);

        for( FQAN fqanWithGroup : fqansWithGroup) {
            String expectedString = "/" + VO_NAME + "/" + GROUP_NAME;
            assertEquals( "Testing " + fqanWithGroup, expectedString,
                    fqanWithGroup.getGroup());
        }
    }

    @Test
    public void testGetGroupForFqansWithoutGroup() {
        FQAN[] fqansWithoutGroup = buildFqanArrayWithNoneOf( FqanElements.GROUP);
        String expectedGroup = "/" + VO_NAME;

        for( FQAN fqanWithoutGroup : fqansWithoutGroup) {
            assertEquals( "Testing " + fqanWithoutGroup, expectedGroup,
                    fqanWithoutGroup.getGroup());
        }
    }

    @Test
    public void testGetRoleForFqansWithRole() {
        FQAN[] fqansWithRole = buildFqanArrayWith( FqanElements.ROLE);

        for( FQAN fqanWithRole : fqansWithRole) {
            assertEquals( "Testing " + fqanWithRole, ROLE_NAME, fqanWithRole
                    .getRole());
        }
    }

    @Test
    public void testGetRoleForFqansWithoutRole() {
        FQAN[] fqansWithoutRole = buildFqanArrayWithNoneOf( FqanElements.ROLE);

        for( FQAN fqanWithoutRole : fqansWithoutRole) {
            assertEquals( "Testing " + fqanWithoutRole, EMPTY_STRING,
                    fqanWithoutRole.getRole());
        }
    }

    @Test
    public void testGetCapabilityForFqansWithCapability() {
        FQAN[] fqansWithCapability =
                buildFqanArrayWith( FqanElements.CAPABILITY);

        for( FQAN fqanWithCapability : fqansWithCapability) {
            assertEquals( "Testing " + fqanWithCapability, CAPABILITY_NAME,
                    fqanWithCapability.getCapability());
        }
    }


    @Test
    public void testGetCapabilityForFqansWithoutCapability() {
        FQAN[] fqansWithoutCapability =
                buildFqanArrayWithNoneOf( FqanElements.CAPABILITY);

        for( FQAN fqanWithoutCapability : fqansWithoutCapability) {
            assertEquals( "Testing " + fqanWithoutCapability, EMPTY_STRING,
                    fqanWithoutCapability.getCapability());
        }
    }

    @Test
    public void testHasRoleForFqanWithNoRole() {
        FQAN[] fqansWithoutRole =
            buildFqanArrayWithNoneOf( FqanElements.ROLE);

        for( FQAN fqanWithoutRole : fqansWithoutRole) {
            assertFalse( "Testing " + fqanWithoutRole, fqanWithoutRole.hasRole());
        }
    }

    @Test
    public void testHasRoleForFqanWithRole() {
        FQAN[] fqansWithRole =
            buildFqanArrayWith( FqanElements.ROLE);

        for( FQAN fqanWithRole : fqansWithRole) {
            assertTrue( "Testing " + fqanWithRole, fqanWithRole.hasRole());
        }
    }

    @Test
    public void testHasCapabilityForFqanWithNoCapability() {
        FQAN[] fqansWithoutCapability =
            buildFqanArrayWithNoneOf( FqanElements.CAPABILITY);

        for( FQAN fqanWithoutCapability : fqansWithoutCapability) {
            assertFalse( "Testing " + fqanWithoutCapability, fqanWithoutCapability.hasCapability());
        }
    }

    @Test
    public void testHasCapabilityForFqanWithCapability() {
        FQAN[] fqansWithCapability =
            buildFqanArrayWith( FqanElements.CAPABILITY);

        for( FQAN fqanWithCapability : fqansWithCapability) {
            assertTrue( "Testing " + fqanWithCapability, fqanWithCapability.hasCapability());
        }
    }


    /*
     * TEST for Role=NULL and Capability=NULL
     */

    @Test
    public void testRoleNullAndNoCapabilityEqualsWithoutRole() {
        FQAN[] fqansWithoutRoleOrCapability =
            buildFqanArrayWithNoneOf( EnumSet.of( FqanElements.ROLE, FqanElements.CAPABILITY));

        for(FQAN fqan : fqansWithoutRoleOrCapability) {
            FQAN fqanWithNullRole = new FQAN(fqan.toString() + "/Role=NULL");
            assertTrue("Testing " + fqan + " equals " + fqanWithNullRole, fqan.equals( fqanWithNullRole));
            assertTrue("Testing " + fqanWithNullRole + " equals " + fqan, fqanWithNullRole.equals( fqan));
        }
    }

    @Test
    public void testRoleNullAndCapabilityEqualsWithoutRole() {
        FQAN[] fqansWithoutRoleOrCapability =
            buildFqanArrayWithNoneOf( EnumSet.of( FqanElements.ROLE, FqanElements.CAPABILITY));

        for(FQAN fqan : fqansWithoutRoleOrCapability) {
            String capabilitySuffix = "/Capability=" + CAPABILITY_NAME;

            FQAN fqanWithoutNullRole = new FQAN(fqan + capabilitySuffix );
            FQAN fqanWithNullRole = new FQAN(fqan + "/Role=NULL" + capabilitySuffix );
            assertTrue("Testing " + fqanWithNullRole, fqanWithoutNullRole.equals( fqanWithNullRole));
            assertTrue("Testing " + fqanWithNullRole, fqanWithNullRole.equals( fqanWithoutNullRole));
        }
    }

    @Test
    public void testCapabilityNullEqualsWithout() {
        FQAN[] fqansWithoutRoleOrCapability =
            buildFqanArrayWithNoneOf( FqanElements.CAPABILITY);

        for(FQAN fqan : fqansWithoutRoleOrCapability) {
            FQAN fqanWithNullCapability = new FQAN(fqan.toString() + "/Capability=NULL");
            assertTrue("Testing " + fqan, fqan.equals( fqanWithNullCapability));
            assertTrue("Testing " + fqan, fqanWithNullCapability.equals( fqan));
        }
    }

    @Test
    public void testGetRoleForRoleNull() {
        FQAN[] fqansWithoutRoleOrCapability =
            buildFqanArrayWithNoneOf( EnumSet.of( FqanElements.ROLE, FqanElements.CAPABILITY));

        for(FQAN fqan : fqansWithoutRoleOrCapability) {
            FQAN fqanWithNullRole = new FQAN(fqan.toString() + "/Role=NULL");
            assertEquals("Checking " + fqanWithNullRole, "", fqanWithNullRole.getRole());
        }
    }

    @Test
    public void testGetCapabilityForCapabilityNull() {
        FQAN[] fqansWithoutCapability =
            buildFqanArrayWithNoneOf( FqanElements.CAPABILITY);

        for(FQAN fqan : fqansWithoutCapability) {
            FQAN fqanWithNullCapability = new FQAN(fqan + "/Capability=NULL");
            assertEquals("Testing " + fqanWithNullCapability, "", fqanWithNullCapability.getCapability());
        }
    }

    @Test
    public void testToStringForRoleNullAndNoCapability() {
        FQAN[] fqansWithoutRoleOrCapability =
            buildFqanArrayWithNoneOf( EnumSet.of( FqanElements.ROLE, FqanElements.CAPABILITY));

        for(FQAN fqan : fqansWithoutRoleOrCapability) {
            FQAN fqanWithNullRole = new FQAN(fqan.toString() + "/Role=NULL");
            assertEquals("Testing " + fqan, fqan.toString(), fqanWithNullRole.toString());
        }
    }

    @Test
    public void testToStringForRoleNullAndSomeCapability() {
        FQAN[] fqansWithoutRoleOrCapability =
            buildFqanArrayWithNoneOf( EnumSet.of( FqanElements.ROLE, FqanElements.CAPABILITY));

        for(FQAN fqan : fqansWithoutRoleOrCapability) {
            String capabilitySuffix = "/Capability=" + CAPABILITY_NAME;

            FQAN fqanWithoutNullRole = new FQAN(fqan + capabilitySuffix );
            FQAN fqanWithNullRole = new FQAN(fqan + "/Role=NULL" + capabilitySuffix );
            assertEquals("Testing " + fqanWithoutNullRole, fqanWithoutNullRole.toString(), fqanWithNullRole.toString());
        }
    }

    @Test
    public void testToStringForCapabilityNull() {
        FQAN[] fqansWithoutRoleOrCapability =
            buildFqanArrayWithNoneOf( FqanElements.CAPABILITY);

        for(FQAN fqan : fqansWithoutRoleOrCapability) {
            FQAN fqanWithNullCapability = new FQAN(fqan.toString() + "/Capability=NULL");
            assertEquals("Testing " + fqan, fqan.toString(), fqanWithNullCapability.toString());
        }
    }

    @Test
    public void testIsValidForValidFqans() {
        for( FQAN fqan : ALL_TYPES) {
            assertTrue( "Checking " + fqan + " is valid", FQAN.isValid( fqan.toString()));
        }
    }

    @Test
    public void testIsValidForInvalidFqans() {
        for( String invalidFqan : INVALID_FQANS) {
            assertFalse( "Checking " + invalidFqan + " is invalid", FQAN.isValid( invalidFqan));
        }
    }


    private void assertNoneEqual( FQAN[] others) {
        for( FQAN fqan1 : ALL_TYPES) {
            for( FQAN fqan2 : others) {
                assertFalse( "Testing " + fqan1 + " not equal to " + fqan2, fqan1.equals( fqan2));
                assertFalse( "Testing " + fqan2 + " not equal to " + fqan1, fqan2.equals( fqan1));
            }
        }
    }

    private static FQAN[] buildFqanArrayOfAllTypes() {
        return buildFqanArrayOfAllWith( VO_NAME, GROUP_NAME, ROLE_NAME, CAPABILITY_NAME,
                EnumSet.allOf( FqanElements.class));
    }

    private static FQAN[] buildFqanArrayWith( FqanElements allWith) {
        return buildFqanArrayOfAllWith( VO_NAME, GROUP_NAME, ROLE_NAME, CAPABILITY_NAME,
                EnumSet.of( allWith));
    }

    private static FQAN[] buildFqanArrayWithNoneOf( FqanElements allWithout) {
        return buildFqanArray( VO_NAME, GROUP_NAME, ROLE_NAME, CAPABILITY_NAME,
                EnumSet.allOf( FqanElements.class), EnumSet.of( allWithout));
    }

    private static FQAN[] buildFqanArrayWithNoneOf( Set<FqanElements> allWithout) {
        return buildFqanArray( VO_NAME, GROUP_NAME, ROLE_NAME, CAPABILITY_NAME,
                EnumSet.allOf( FqanElements.class), allWithout);
    }

    private static FQAN[] buildFqanArrayWithOtherVo( String voName) {
        return buildFqanArrayOfAllWith( voName, GROUP_NAME, ROLE_NAME, CAPABILITY_NAME,
                EnumSet.of( FqanElements.VO));
    }

    private static FQAN[] buildFqanArrayWithOtherGroup( String groupName) {
        return buildFqanArrayOfAllWith( VO_NAME, groupName, ROLE_NAME, CAPABILITY_NAME,
                EnumSet.of( FqanElements.GROUP));
    }

    private static FQAN[] buildFqanArrayWithOtherRole( String roleName) {
        return buildFqanArrayOfAllWith( VO_NAME, GROUP_NAME, roleName, CAPABILITY_NAME,
                EnumSet.of( FqanElements.ROLE));
    }

    private static FQAN[] buildFqanArrayWithOtherCapability( String capabilityName) {
        return buildFqanArrayOfAllWith( VO_NAME, GROUP_NAME, ROLE_NAME, capabilityName,
                EnumSet.of( FqanElements.CAPABILITY));
    }

    private static FQAN[] buildFqanArrayOfAllWith(String voName, String groupName,
                                  String roleName, String capabilityName,
                                  Set<FqanElements> allWith) {
        return buildFqanArray( voName, groupName, roleName, capabilityName, allWith,
                EnumSet.noneOf( FqanElements.class));
    }

    /**
     * Build an array of FQANs by considering the supplied VO name, group
     * name, role name and capability name. The resulting FQANs have the
     * form:
     *
     *<pre>
     *   "/" voName ["/" groupName] ["/Role=" roleName] ["/Capability=" capabilityName]
     *</pre>
     *
     * The allWith argument is used to control the list of FQANs returned.
     * Only those FQANs that include some type corresponding to an element in
     * the allWith Set are included in the returned array; for example, specifying
     * allWith with ROLE as its single element, {ROLE}, returns an array with
     * only those FQANs with a "/Role=" value:
     *
     *<pre>
     *   /voName/Role=roleName
     *   /voName/Role=roleName/Capability=capabilityName
     *   /voName/groupName/Role=roleName
     *   /voName/groupName/Role=roleName/Capability=capabilityName
     *</pre>
     *
     * The allWithout argument further controls the output so that the returned array
     * of FQANs will not include any that have an FqanElement included in the
     * allWithout Set.
     */
    private static FQAN[] buildFqanArray( String voName, String groupName,
                                          String roleName, String capabilityName,
                                          Set<FqanElements> allWith,
                                          Set<FqanElements> allWithout) {
        List<FQAN> fqans = new ArrayList<FQAN>();

        String voFqan = "/" + voName;
        String voAndGroup = voFqan + "/" + groupName;

        String roleSuffix = "/Role=" + roleName;
        String capabilitySuffix = "/Capability=" + capabilityName;

        if( isIncludeable( allWith, allWithout, EnumSet.of( FqanElements.VO))) {
            fqans.add( new FQAN( voFqan));
        }

        if( isIncludeable( allWith, allWithout, EnumSet.of( FqanElements.VO,
                FqanElements.ROLE))) {
            fqans.add( new FQAN( voFqan + roleSuffix));
        }

        if( isIncludeable(allWith, allWithout, EnumSet.of(FqanElements.VO,
                FqanElements.CAPABILITY))) {
            fqans.add( new FQAN( voFqan + capabilitySuffix));
        }

        if( isIncludeable( allWith, allWithout, EnumSet.of( FqanElements.VO,
                FqanElements.ROLE, FqanElements.CAPABILITY))) {
            fqans.add( new FQAN( voFqan + roleSuffix + capabilitySuffix));
        }

        if( isIncludeable( allWith, allWithout, EnumSet.of( FqanElements.VO,
                FqanElements.GROUP))) {
            fqans.add( new FQAN( voAndGroup));
        }

        if( isIncludeable( allWith, allWithout, EnumSet.of( FqanElements.VO,
                FqanElements.GROUP, FqanElements.ROLE))) {
            fqans.add( new FQAN( voAndGroup + roleSuffix));
        }

        if( isIncludeable(allWith, allWithout, EnumSet.of(FqanElements.VO,
                FqanElements.GROUP, FqanElements.CAPABILITY))) {
            fqans.add( new FQAN( voAndGroup + capabilitySuffix));
        }

        if( isIncludeable( allWith, allWithout, EnumSet.of( FqanElements.VO,
                FqanElements.GROUP, FqanElements.ROLE, FqanElements.CAPABILITY))) {
            fqans.add( new FQAN( voAndGroup + roleSuffix + capabilitySuffix));
        }

        return fqans.toArray( new FQAN[fqans.size()]);
    }

    private static boolean isIncludeable( Set<FqanElements> allWith,
                                          Set<FqanElements> allWithout,
                                          Set<FqanElements> present) {
        boolean isIncludeableFromAllWith = !Collections.disjoint( allWith, present);
        boolean isIncludeableNotAllWithout = Collections.disjoint( allWithout, present);

        return isIncludeableFromAllWith && isIncludeableNotAllWithout;
    }
}
