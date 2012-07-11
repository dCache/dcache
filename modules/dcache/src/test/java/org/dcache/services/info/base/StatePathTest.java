package org.dcache.services.info.base;


import static org.junit.Assert.*;


import org.junit.Test;
import org.dcache.services.info.base.StatePath;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class StatePathTest extends InfoBaseTestHelper {

    // The number of elements in the array must be prime for stride to work
    private static final String PATH_ELEMENTS[] = {"foo", "bar", "baz"};

    /**
     * Test that the String constructor works (or, at least, is
     * not obviously broken).
     * <p>
     * Assumes that hashCode() works.
     */
    @Test
    public void testSimpleRepeatedHashCode() {
        Set<Integer> seenHashCodes = new TreeSet<Integer>();

        for( String element : PATH_ELEMENTS) {
            StatePath testPath = new StatePath( element);

            assertFalse( "hashCode for " + element + " repeated",
                    hashAlreadySeen( seenHashCodes, testPath.hashCode()));

            // Make sure that non-intern Strings hash to same value.
            testPath = new StatePath( new String( element));

            assertTrue( "hashCode for " + element + " not found again",
                    hashAlreadySeen( seenHashCodes, testPath.hashCode()));
        }
    }


    /**
     *  Test that all ordering of elements produces distinct hashes.
     */
    @Test
    public void testOrderedElementHashCode() {
        Set<Integer> seenHashCodes = new TreeSet<Integer>();

        final int MAX_LEN = 5;

        for( int len = 2; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++ ) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {

                    StatePath path = buildStatePath( len, offset, stride);

                    assertFalse( "ordered elements test failed due to hashCode repeating for len="+len+", offset="+offset+", stride="+stride+" (" + path.toString()+")",
                            hashAlreadySeen( seenHashCodes, path.hashCode()));
                }
            }
        }
    }


    /**
     * Test some potentially awkward corner cases
     */
    @Test
    public void testAwkwardHashCode() {
        StatePath nullPath = new StatePath( (String)null);
        StatePath emptyPath = new StatePath( "");
        StatePath spacePath = new StatePath( " ");

        assertNotSame( "null and empty paths hash to same value", nullPath.hashCode(), emptyPath.hashCode());
        assertNotSame( "empty and single-space paths hash to same value", spacePath.hashCode(),  emptyPath.hashCode());
        assertNotSame( "null and single-space paths hash to same value", spacePath.hashCode(), nullPath.hashCode());
    }


    /**
     *  Check that parsing results in a StatePath that gives the same result
     *  as building the StatePath manually.
     * 
     *  This assumes hashCode() and newChild() work.
     */
    @Test
    public void testParsePath() {
        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {

                    String pathElement[] = buildPathArray( len, offset, stride);
                    StatePath manuallyBuiltPath = buildStatePath( len, offset, stride);

                    StringBuffer sb = new StringBuffer();
                    for( String element : pathElement) {
                        if( sb.length() > 0) {
                            sb.append(".");
                        }
                        sb.append( element);
                    }

                    // parse the string
                    StatePath parsedPath = StatePath.parsePath( sb.toString());

                    assertTrue( "parsed path ("+parsedPath.toString()+") hashes differently to manually built path ("+ manuallyBuiltPath.toString() +")", parsedPath.hashCode() == manuallyBuiltPath.hashCode());
                }
            }
        }
    }

    /**
     * Test some obvious, awkward cases.  Really, we just check that
     * these don't crash.  Correct functionality is checked elsewhere.
     */
    @Test
    public void testStatePathString() {
        StatePath nullPath = new StatePath( (String)null);
        StatePath emptyPath = new StatePath( "");
        StatePath spacePath = new StatePath( " ");

        assertNotNull( "StatePath(null) returned null", nullPath);
        assertNotNull( "StatePath(\"\") returned null", emptyPath);
        assertNotNull( "StatePath(\" \") returned null", spacePath);
    }


    /**
     * Fairly exhaustive test of the equals() method.
     */
    @Test
    public void testEqualsObject() {
        final int MAX_LEN = 4;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {

                    StatePath path1 = buildStatePath( len, offset, stride);

                    assertFalse( "path1 equals null", path1.equals( null));
                    assertEquals( "path1 equals path1", path1, path1);

                    // Iterate over all other StatePaths, checking equality.
                    for( int lenOther = 1; lenOther < MAX_LEN; lenOther++) {
                        for( int offsetOther = 0; offsetOther < PATH_ELEMENTS.length; offsetOther++) {
                            for( int strideOther = 0; strideOther < PATH_ELEMENTS.length; strideOther++) {

                                StatePath path2 = buildStatePath( lenOther, offsetOther, strideOther);

                                /**
                                 * What we would expect for equality.  The slightly odd stride-ness is because,
                                 * when len == 1, stride is irrelevant.
                                 */
                                boolean expectedEquality = len == lenOther && offset == offsetOther && (len == 1 || stride == strideOther);

                                if( expectedEquality) {
                                    assertEquals( "path1 not equal to path2", path1, path2);
                                    assertEquals( "path2 not equal to path1", path2, path1);
                                } else {
                                    assertFalse( "path1 equal to path2", path1.equals( path2));
                                    assertFalse( "path2 equal to path1", path2.equals( path1));
                                }

                            }
                        }
                    }
                }
            }
        }
    }



    /**
     * Check that equals or is child returns the correct answer.
     */
    @Test
    public void testEqualsOrHasChild() {
        final int MAX_LEN = 4;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {

                    StatePath parentPath = buildStatePath( len, offset, stride);

                    for( int lenOther = 1; lenOther < MAX_LEN; lenOther++) {
                        for( int offsetOther = 0; offsetOther < PATH_ELEMENTS.length; offsetOther++) {
                            for( int strideOther = 0; strideOther < PATH_ELEMENTS.length; strideOther++) {

                                StatePath childPath = buildStatePath( lenOther, offsetOther, strideOther);

                                boolean expected = len <= lenOther && offset == offsetOther && (len == 1 || stride == strideOther);

                                if( expected) {
                                    assertTrue( "false: " + parentPath.toString() + " equalsOrHasChild:" + childPath.toString(),
                                            parentPath.equalsOrHasChild( childPath));
                                } else {
                                    assertFalse( "true: " + parentPath.toString() + " equalsOrHasChild:" + childPath.toString(),
                                            parentPath.equalsOrHasChild( childPath));
                                }
                            }
                        }
                    }
                }
            }
        }
    }


    @Test
    public void testEqualsOrHasChildNotEqualAsNull() {
        StatePath path = StatePath.parsePath("aa.bb");

        assertFalse( "null is unexpectedly equal-or-child-of a path", path.equalsOrHasChild( null));
    }

    @Test
    public void testNullIsParentOf() {
        StatePath path = StatePath.parsePath("aa.bb");

        assertFalse( "StatePath claiming to be parent of root", path.isParentOf(null));
    }

    /**
     * Check that isParentOf() works as expected.
     */
    @Test
    public void testIsParentOf() {
        final int MAX_LEN = 4;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {

                    StatePath parentPath = buildStatePath( len, offset, stride);

                    for( int lenOther = 1; lenOther < MAX_LEN; lenOther++) {
                        for( int offsetOther = 0; offsetOther < PATH_ELEMENTS.length; offsetOther++) {
                            for( int strideOther = 0; strideOther < PATH_ELEMENTS.length; strideOther++) {

                                StatePath childPath = buildStatePath( lenOther, offsetOther, strideOther);
                                assertNotNull( "childPath", childPath);

                                boolean expected = (len+1 == lenOther) && offset == offsetOther && (len == 1 || stride == strideOther);

                                if( expected) {
                                    assertTrue( "false: " + parentPath.toString() + " isParentOf:" + childPath.toString(),
                                            parentPath.isParentOf( childPath));
                                } else {
                                    assertFalse( "true: " + parentPath.toString() + " isParentOf:" + childPath.toString(),
                                            parentPath.isParentOf( childPath));
                                }
                            }
                        }
                    }
                }
            }
        }
    }


    @Test
    public void testToString() {
        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {

                    String elements[] = buildPathArray( len, offset, stride);
                    StatePath path = buildStatePath( len, offset, stride);

                    StringBuffer sb = new StringBuffer();

                    for( String element : elements) {
                        if( sb.length() > 0) {
                            sb.append(".");
                        }
                        sb.append( element);
                    }

                    assertEquals( "path name mismatch: " + path.toString()+ " != " + sb.toString(),
                            path.toString(), sb.toString());
                }
            }
        }
    }

    @Test
    public void testToStringPrefix() {
        StatePath testPath = StatePath.parsePath("aa.bb.cc");

        assertEquals( "toString with prefix", "aa.bb.cc", testPath.toString( null));
        assertEquals( "toString with prefix", "bb.cc", testPath.toString( StatePath.parsePath( "foo")));
        assertEquals( "toString with prefix", "cc", testPath.toString( StatePath.parsePath( "foo.bar")));
        assertEquals( "toString with prefix", "", testPath.toString( StatePath.parsePath( "foo.bar.baz")));
    }



    @Test
    public void testToStringStringInt() {

        String seperators[] = { ".", "\"", "[]", "", "t"};

        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {

                    String elements[] = buildPathArray( len, offset, stride);
                    StatePath path = buildStatePath( len, offset, stride);

                    for( String sep : seperators) {

                        for( int skip = 0; skip < len+1; skip++) {
                            StringBuffer sb = new StringBuffer();
                            int count=0;

                            for( String element : elements) {
                                if( count++ < skip) {
                                    continue;
                                }

                                if( sb.length() > 0) {
                                    sb.append(sep);
                                }
                                sb.append( element);
                            }

                            String pathStr = path.toString( sep, skip);

                            assertEquals( "path "+path.toString( sep, skip)+" (skip="+skip+") not equal to expected value " + sb.toString(),
                                    pathStr, sb.toString());
                        }
                    }

                }
            }
        }
    }


    /**
     * TODO: implement this test
	@Test
	public void testToStringStatePath() {
		fail("Not yet implemented");
	}
     */


    @Test
    public void testGetFirstElement() {
        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                    StatePath path = buildStatePath( len, offset, stride);

                    String firstElement = path.getFirstElement();
                    assertNotNull( "firstElement", firstElement);

                    assertEquals( "mismatch between first element ("+firstElement+") and expected value (" + PATH_ELEMENTS [offset] + ")", firstElement, PATH_ELEMENTS [offset]);
                }
            }
        }
    }


    @Test
    public void testGetLastElement() {
        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                    StatePath path = buildStatePath( len, offset, stride);
                    String pathElements[] = buildPathArray( len, offset, stride);

                    String lastElement = path.getLastElement();
                    String expectedLastElement = pathElements [pathElements.length -1];

                    assertEquals( "mismatch between last element ("+lastElement+") and expected value (" + expectedLastElement + ")", lastElement, expectedLastElement);
                }
            }
        }
    }


    @Test
    public void testNewChildString() {
        final int MAX_LEN = 5;
        final String childElement = "foo";

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                    StatePath parent = buildStatePath( len, offset, stride);

                    StatePath child = parent.newChild( childElement);

                    assertTrue( "false: parent isParentOf child", parent.isParentOf( child));
                    assertFalse( "true: child isParentOf parent", child.isParentOf( parent));
                    assertEquals( "last element mismatch", child.getLastElement(), childElement);
                }
            }
        }
    }


    @Test
    public void testNewChildStatePath() {
        final int MAX_LEN = 3;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                    StatePath parent = buildStatePath( len, offset, stride);

                    for( int lenOther = 1; lenOther < MAX_LEN; lenOther++) {
                        for( int offsetOther = 0; offsetOther < PATH_ELEMENTS.length; offsetOther++) {
                            for( int strideOther = 0; strideOther < PATH_ELEMENTS.length; strideOther++) {
                                StatePath subPath = buildStatePath( lenOther, offsetOther, strideOther);

                                StatePath child = parent.newChild( subPath);

                                if( lenOther == 1) {
                                    assertTrue("false: parent (" + parent
                                            .toString() + ") isParentOf child (" + child
                                            .toString() + ")", parent
                                            .isParentOf(child));
                                } else {
                                    assertFalse("true: parent (" + parent
                                            .toString() + ") isParentOf child (" + child
                                            .toString() + ")", parent
                                            .isParentOf(child));
                                }

                                assertFalse( "true: child isParentOf parent", child.isParentOf( parent));

                                String expectedChildPath = parent.toString() + "." + subPath.toString();

                                assertEquals( "mismatch between expected ("+expectedChildPath+") and actual path ("+child.toString()+")", expectedChildPath, child.toString());
                            }
                        }
                    }
                }
            }
        }
    }



    @Test
    public void testChildPath() {
        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                    StatePath path = buildStatePath( len, offset, stride);

                    StatePath childsPath = path.childPath();

                    if( len == 1) {
                        assertSame( "child of single element wrong", childsPath, null);
                    } else {
                        // Try to rebuild the original path
                        StatePath first = new StatePath( PATH_ELEMENTS [offset]);
                        StatePath reconstructedPath = first.newChild( childsPath);

                        assertEquals( "unable to reconstruct original path", reconstructedPath, path);
                    }
                }
            }
        }
    }



    @Test
    public void testParentPath() {

        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                    StatePath path = buildStatePath( len, offset, stride);
                    String array[] = buildPathArray( len, offset, stride);

                    StatePath parentPath = path.parentPath();

                    if( len == 1) {
                        assertSame( "parentPath returned non-null entry for path with a single element", parentPath, null);

                    } else {
                        // Attempt to reconstruct the original path
                        StatePath reconstructedPath = parentPath.newChild( array [array.length-1]);
                        assertEquals( "reconstructedPath and path not the same", reconstructedPath, path);
                    }

                }
            }
        }

    }


    @Test
    public void testIsSimplePath() {

        final int MAX_LEN = 5;

        for( int len = 1; len < MAX_LEN; len++) {
            for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
                for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                    StatePath path = buildStatePath( len, offset, stride);

                    if( len == 1) {
                        assertTrue("expected true", path.isSimplePath());
                    } else {
                        assertFalse("expected false", path.isSimplePath());
                    }
                }
            }
        }
    }


    @Test
    public void testParseAndListFromStringCopyEqual() {
        String elementName = "element-1";
        StatePath pathFromParser = StatePath.parsePath( elementName);

        String copyOfElementName = new String(elementName);
        assertNotSame( "asserting String not intern()ed", elementName, copyOfElementName);
        StatePath pathFromList = StatePath.buildFromList( Arrays.asList( copyOfElementName));

        assertEquals( "", pathFromParser, pathFromList);
    }

    @Test
    public void testListAndParseFromStringCopyEqual() {
        String elementName = "element-1";
        StatePath pathFromList = StatePath.buildFromList( Arrays.asList( elementName));

        String copyOfElementName = new String(elementName);
        assertNotSame( "asserting String not intern()ed", elementName, copyOfElementName);
        StatePath pathFromParser = new StatePath(elementName);

        assertEquals( "", pathFromParser, pathFromList);
    }

    /**
     *   S A N I T Y---C H E C K   T E S T S
     */


    /**
     * Test the vital buildPathArray() method.
     */
    @Test
    public void testPathArray() {

        // Test that offset works and that, when length is 1, the stride is irrelevant.
        for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
            for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                String elements[] = buildPathArray( 1, offset, stride);

                assertEquals( "single-element buildStatePath broken", elements[0], PATH_ELEMENTS [offset]);
            }
        }

        // Test that stride works, at least for paths of length 2
        for( int offset = 0; offset < PATH_ELEMENTS.length; offset++) {
            for( int stride = 0; stride < PATH_ELEMENTS.length; stride++) {
                String elements[] = buildPathArray( 2, offset, stride);

                if( stride == 0) {
                    assertEquals("mismatch between first and second elements", elements[0], elements[1]);
                } else {
                    assertFalse("first and second elements are the same when they shouldn't be", elements[0]
                            .equals(elements[1]));
                }
            }
        }
    }


    /**
     * Check that that hashAlreadySeen works as expected.
     * (or, at least, not obviously broken).
     */
    @Test
    public void testHashAlreadySeen() {
        Set<Integer> seenHashCodes = new TreeSet<Integer>();

        for( int hash = 0; hash < 100; hash++) {
            assertFalse(hashAlreadySeen(seenHashCodes, hash));
        }

        for( int hash = 0; hash < 100; hash++) {
            assertTrue(hashAlreadySeen(seenHashCodes, hash));
        }
    }





    /**
     *   P R I V A T E   F U N C T I O N S
     */

    /**
     * Provide a path of length len by cycling through the available path elements.
     * Which elements are selected is controlled by the offset and stride parameters.
     * <p>
     * This assumes that String constructor and newChild(String) method is working.
     * @param len number of elements in the path
     * @param offset the index of the first element to be selected
     * @param stride the number to increment the index after each iteration
     * @return a StatePath with the number of elements present.
     */
    private StatePath buildStatePath( int len, int offset, int stride) {
        StatePath path = null;

        String elements[] = buildPathArray( len, offset, stride);

        for( String element : elements) {
            path = path == null ? new StatePath(element) : path
                    .newChild(element);
        }

            if( len > 0) {
                assertNotNull("buildStatePath", path);
            } else {
                assertNull("buildStatePath", path);
            }

            return path;
    }

    /**
     * Provide a String array based on the PATH_ELEMENTS by cycling through the available
     * path elements.  The selected elements are controlled by the offset and stride
     * parameters.
     * <p>
     * @param len number of elements in the array,
     * @param offset the index of the first element to be selected
     * @param stride the number to increment the index after each iteration
     * @return a String array with the number of elements.
     */
    private String[] buildPathArray( int len, int offset, int stride) {
        String[] array = new String[len];

        for( int arrayIdx = 0; arrayIdx < len; arrayIdx++) {
            array[arrayIdx] = PATH_ELEMENTS[(arrayIdx * stride + offset) % PATH_ELEMENTS.length];
        }

        return array;
    }

}
