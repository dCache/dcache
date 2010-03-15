package org.dcache.services.info.base;


import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

/**
 * Test that StringStateValue behaves as expected.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StringStateValueTest extends InfoBaseTestHelper {

	private static final String[] TEST_STRINGS = { null, "", "foo", "bar", "baz"};

	@Test
	public void testHashCode() {
		Set<Integer> seenHashCodes = new TreeSet<Integer>();

		for( String testString : TEST_STRINGS) {
			StringStateValue testValue = new StringStateValue( testString);

			assertFalse( "hashCode for " + testString + " repeated",
					hashAlreadySeen( seenHashCodes, testValue.hashCode()));

			StringStateValue testValue2 = new StringStateValue( testString);

			assertTrue( "hashCode for " + testString + " not found again",
					hashAlreadySeen( seenHashCodes, testValue2.hashCode()));
		}
	}


	@Test
	public void testGetTypeName() {

		// We do it a few times, just in case.
		for( String testString : TEST_STRINGS) {
			StringStateValue testValue = new StringStateValue( testString);
			assertEquals( testValue.getTypeName(), "string");
		}
	}


	@Test
	public void testAcceptVisitorStatePathStateVisitor() {

		class TestStringVisitor implements StateVisitor {
			boolean broken;
			String foundValue;
			StatePath foundPath;
			@Override
			public void visitString( StatePath path, StringStateValue value) {
			    foundPath = path;
			    foundValue = value.toString();
			}
            @Override
			public void visitInteger( StatePath path, IntegerStateValue value)  {
                broken = true;
            }
            @Override
			public void visitBoolean( StatePath path, BooleanStateValue value) {
                broken = true;
            }
            @Override
			public void visitFloatingPoint( StatePath path, FloatingPointStateValue value) {
                broken = true;
            }
            @Override
			public void visitCompositePreDescend( StatePath path, Map<String,String> metadata) {
                broken = true;
            }
            @Override
			public void visitCompositePostDescend( StatePath path, Map<String,String> metadata) {
                broken = true;
            }
            @Override
            public boolean isVisitable( StatePath path) {
                return true;
            }
		}

		StatePath path = new StatePath( "path");

		for( String testString : TEST_STRINGS) {

			StringStateValue testValue = new StringStateValue( testString);

			TestStringVisitor testVisitor = new TestStringVisitor();

			testValue.acceptVisitor( path, testVisitor);

			assertFalse( "visitor called back on a method it shouldn't have", testVisitor.broken);
			assertEquals( "visitor failed to discover string", expectedToStringValue( testString), testVisitor.foundValue);
			assertEquals( "visitor failed to return equal StatePath", path, testVisitor.foundPath);
		}
	}



	@Test
	public void testEqualsObject() {

		for( String firstString : TEST_STRINGS) {

			StringStateValue firstSsv = new StringStateValue( firstString);

			for( String secondString : TEST_STRINGS) {

				StringStateValue secondSsv = new StringStateValue( secondString);
				boolean shouldBeEqual = false;

				// Somehow getting this correct without getting a NullPointerException is difficult.
				if( firstString == null) {
					if( secondString == null)
						shouldBeEqual = true;
				} else {
					shouldBeEqual = firstString.equals( secondString);
				}

				if( shouldBeEqual) {
					assertEquals( "two StringStateValues same creating String, first.equals(second)", firstSsv, secondSsv);
					assertEquals( "two StringStateValues same creating String, second.equals(first)", secondSsv, firstSsv);
				} else {
					assertFalse( "two StringStateValues different creating String, first.equals(second)", firstSsv.equals( secondSsv));
					assertFalse( "two StringStateValues different creating String, second.equals(first)", secondSsv.equals( firstSsv));
				}
			}
		}
	}

	@Test
	public void testStringStateValueString() {
		for( String stringVal : TEST_STRINGS)
			assertIsEphemeral( "creating with a regular string: " + stringVal, new StringStateValue( stringVal));
	}


	@Test
	public void testStringStateValueStringBoolean() {
		for( String stringVal : TEST_STRINGS)
			assertIsEphemeral( "creating with a regular string: " + stringVal, new StringStateValue( stringVal, false));
		for( String stringVal : TEST_STRINGS)
			assertIsImmortal( "creating with a regular string: " + stringVal, new StringStateValue( stringVal, true));
	}


	@Test
	public void testStringStateValueStringLong() {
		for( long duration = -1; duration < 3; duration++) {
			assertIsMortal( "str: (null) + " + Long.toString(duration), new StringStateValue( null, duration), duration < 0 ? 0 : duration);
			assertIsMortal( "str: \"\" + " + Long.toString(duration), new StringStateValue( "", duration), duration < 0 ? 0 : duration);

			for( String strVal : TEST_STRINGS)
				assertIsMortal( "str: \"" + strVal + "\" + " + Long.toString(duration), new StringStateValue( strVal, duration), duration < 0 ? 0 : duration);
		}

	}

	@Test
	public void testToString() {
		for( String stringVal : TEST_STRINGS) {
			StringStateValue value = new StringStateValue( stringVal);
			assertEquals( "built from " + stringVal, expectedToStringValue( stringVal), value.toString());
		}
	}


	/**
	 * What String we expect to see when calling toString()
	 * @param buildString the String used to build the StringStateValue
	 * @return the expected result from toString()
	 */
	private String expectedToStringValue( String buildString) {
		return buildString != null ? buildString : "(null)";
	}

}
