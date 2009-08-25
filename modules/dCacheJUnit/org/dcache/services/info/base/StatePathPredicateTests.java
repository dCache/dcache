package org.dcache.services.info.base;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.BitSet;

import org.junit.Test;

public class StatePathPredicateTests {

	private static final String NON_APPEARING_PATH_ELEMENT = "Terpsichore";
	private static final String WILDCARD_ELEMENT = "*";

	@Test
	public void testParsePathString() {

		// Test some likely awkward cases
		StatePathPredicate.parsePath("");
		StatePathPredicate.parsePath(null);
	}


	@Test
	public void testStatePathPredicateStatePath() {
		String elements[] = { "foo", "bar", "baz", "bing", "bong"};

		for( int depth = 1; depth <= elements.length; depth++) {
			StatePath testPath = buildPath( elements, depth);
			assertTrue( "Creating StatePathPredicate with a StatePath ("+testPath+") doesn't match creating StatePath", testBuildStatePath( testPath));
		}
	}

	@Test
	public void testStatePathPredicateString() {

		// Test some likely awkward cases
		new StatePathPredicate( (String)null);
		new StatePathPredicate( "");

		// Test individual cases.
		String[] topElements = {"foo", "bar", "baz"};

		for( String topElement : topElements)
			assertTrue( "predicate match fails for " + topElement, testBuildStatePath( new StatePath( topElement)));
	}

	@Test
	public void testMatches() {

		// element names for the depths
		String[] elements = {"foo", "bar", "baz"};

		for( int depth = 1; depth < elements.length; depth++) {
			StatePath testPath = buildPath( elements, depth);

			StatePathPredicate predicate = new StatePathPredicate( testPath);

			assertFalse( "predicate "+predicate+" matches null path", predicate.matches(null));

			assertTrue( "predicate "+predicate+" doesn't match own path", predicate.matches( testPath));

			StatePath childPath = testPath.newChild( "child");
			assertFalse( "predicate "+predicate+" matches child of path " + childPath, predicate.matches( childPath));

			if( depth > 1) {
				StatePath parentPath = testPath.parentPath();
				assertFalse( "predicate "+predicate+" matches parent "+parentPath, predicate.matches( parentPath));

				// Try replacing the last element of the *path* with a wildcard.  This shouldn't match.
				StatePath lastWild = testPath.parentPath().newChild( WILDCARD_ELEMENT);
				assertFalse( "predicate "+predicate+" matches path "+lastWild, predicate.matches(lastWild));
			}
		}

		// +1 so we can store the extra bit after the final test (all wildcards).
		BitSet wildcards = new BitSet( elements.length+1);

		while( wildcards.length() <= elements.length) {
			StatePathPredicate predicate = buildWildcardPredicate( elements, wildcards);
			StatePath testPath = buildMatchingPath( elements, wildcards);

			assertTrue("Predicate " + predicate.toString() + " failed to match path " + testPath.toString(), predicate.matches( testPath));

			/**
			 * Magic that does a numerical increment.  Note that
			 * numbers ending (. is either 0 or 1).
			 *  .....0 -> .....1  (xor ...001)
			 *  ....01 -> ....10  (xor ...011)
			 *  ...011 -> ...100  (xor ...111)
			 */
			BitSet incr = new BitSet( elements.length+1);
			incr.set(0, 1+wildcards.nextClearBit(0));
			wildcards.xor( incr);
		}
	}

	@Test
	public void testMultipleWildcards() {
		StatePathPredicate predicate = StatePathPredicate.parsePath("aaa.*.bbb.*");
		assertMatching( "multiple wildcards", predicate, StatePath.parsePath("aaa.foo.bbb.bar"));
		assertNotMatching( "multiple wildcards", predicate, StatePath.parsePath("aaa.foo.bbb"));
		assertNotMatching( "multiple wildcards", predicate, StatePath.parsePath("aaa.foo.ccc.bar"));
		assertNotMatching( "multiple wildcards", predicate, StatePath.parsePath("ccc.foo.bbb.bar"));
	}

	private void assertMatching( String msg, StatePathPredicate predicate, StatePath path) {
		assertTrue( msg + "expected matching path " + path + " for " + predicate, predicate.matches( path));
	}

	private void assertNotMatching( String msg, StatePathPredicate predicate, StatePath path) {
		assertFalse( msg + "expected matching path " + path + " for " + predicate, predicate.matches( path));
	}

	@Test
	public void testChildPath() {
		StatePath testPath = StatePath.parsePath( "foo.bar.baz");
		StatePathPredicate predicate = new StatePathPredicate( testPath);

		for( int i = 3; i > 0; i--) {
			assertTrue( "predicate " + predicate + " does not match path " + testPath, predicate.matches(testPath));

			testPath = testPath.childPath();
			predicate = predicate.childPath();

			if( i > 1) {
				assertNotNull("testPath is null for loop " + i, testPath);
				assertNotNull("testPathPredicate is null for loop "+i, predicate);
			} else {
				assertNull( "testPath not null", testPath);
				assertNull( "testPathPredicate not null", predicate);
			}
		}
	}



	@Test
	public void testTopElementMatches() {

		// Check simple strings
		StatePathPredicate nonWildPredicate = StatePathPredicate.parsePath( "aaa.bbb.ccc");

		// Check some awkward cases
		assertFalse( "topElementMatches() returned true for null", nonWildPredicate.topElementMatches( null));
		assertFalse( "topElementMatches() returned true for wildcard character", nonWildPredicate.topElementMatches( WILDCARD_ELEMENT));
		assertFalse( "topElementMatches() returned true for empty string", nonWildPredicate.topElementMatches( ""));

		assertTrue( "topElement failed to match the element", nonWildPredicate.topElementMatches( "aaa"));

		// Check wildcards

		StatePathPredicate wildPredicate = StatePathPredicate.parsePath( "*.bbb.ccc");
		assertTrue( "topElement failed to match the element", wildPredicate.topElementMatches( "aaa"));
		assertTrue( "topElement failed to match the element", wildPredicate.topElementMatches( "bbb"));
	}



	/**
	 *   P R I V A T E   M E T H O D S
	 */


	/**
	 * Build a StatePathPredicate by composing multiple String elements up to the given depth.
	 * If a level has wildcard set, then a wildcard is used instead of that element.
	 * @param elements an array of elements
	 * @param wildcard a BitSet of element-depths to make wild
	 * @return A StatePathPredicate, built from elements, but with wildcard characters introduced.
	 */
	private StatePathPredicate buildWildcardPredicate( String[] elements, BitSet wildcards) {
		StatePath path = null;

		for( int i=0; i < elements.length; i++) {
			String name = wildcards.get(i) ? WILDCARD_ELEMENT : elements [i];
			path = path == null ? new StatePath( name) : path.newChild( name);
		}

		return new StatePathPredicate( path);
	}


	/**
	 * Build a StatePath based on elements array.  However, wherever the wildcards has a
	 * corresponding bit set, that element is substituted by an element that otherwise
	 * doesn't exist.  This, then, will only match if the wildcard is genuinely wild.
	 * @param elements an Array of elements to build the path from.
	 * @param wildcards a bitmap of which path elements to substitute with a wild value
	 * @return a StatePath, built from elements except for those elements with the corresponding bitmap bit is set.
	 */
	private StatePath buildMatchingPath( String[] elements, BitSet wildcards) {
		StatePath path = null;

		for( int i=0; i < elements.length; i++) {
			String name = wildcards.get(i) ? NON_APPEARING_PATH_ELEMENT + "_" + i : elements [i];
			path = path == null ? new StatePath( name) : path.newChild( name);
		}

		return path;
	}


	/**
	 * Build a StatePath from a list of string elements that has depth length.
	 * @param elements the list of elements
	 * @param length the depth of the elements
	 * @return the new StatePath
	 */
	private StatePath buildPath( String[] elements, int length) {
		StatePath path = null;

		assert length <= elements.length;

		for( int i=0; i < length; i++) {
			String name = elements [i];
			path = path == null ? new StatePath( name) : path.newChild( name);
		}

		return path;
	}


	/**
	 * Test that, when building a StatePathPredicate from a StatePath, that the resulting StatePathPredicate
	 * matches the original StatePath.
	 * @param path the StatePath to test
	 * @return true if the resulting StatePathPredicate matches the creating StatePath.
	 */
	private boolean testBuildStatePath( StatePath path) {
		StatePathPredicate predicate = new StatePathPredicate( path);
		return predicate.matches( path);
	}

}
