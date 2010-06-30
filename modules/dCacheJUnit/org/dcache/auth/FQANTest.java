package org.dcache.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import static org.junit.Assert.*;

/**
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

    private static final FQAN ALL_TYPES[] = buildFqanArrayOfAllTypes();

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
