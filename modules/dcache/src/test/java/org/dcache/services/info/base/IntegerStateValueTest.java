package org.dcache.services.info.base;


import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

public class IntegerStateValueTest extends InfoBaseTestHelper {

	private static final int TEST_INTEGERS[] = {-2, -1, 0, 1, 2 };
	private static final int TEST_DURATIONS[] = {-1, 0, 1, 2 };

	/** An integer value never used when testing */
	private static final int NOT_USED_INTEGER = -99;

	@Test
	public void testHashCode() {
		Set<Integer> seenHashCodes = new TreeSet<Integer>();

		for( long testInt : TEST_INTEGERS) {
			IntegerStateValue testValue = new IntegerStateValue( testInt);

			assertFalse( "hashCode for " + testInt + " repeated",
					hashAlreadySeen( seenHashCodes, testValue.hashCode()));

			IntegerStateValue testValue2 = new IntegerStateValue( testInt);

			assertTrue( "hashCode for " + testInt + " not found again",
					hashAlreadySeen( seenHashCodes, testValue2.hashCode()));
		}
	}

	@Test
	public void testGetTypeName() {
		for( int testInt : TEST_INTEGERS) {
			IntegerStateValue testValue = new IntegerStateValue( testInt);

			assertEquals( "unexpected value", "integer", testValue.getTypeName());
		}
	}


	@Test
	public void testAcceptVisitorStatePathStateVisitor() {

		class TestIntegerVisitor implements StateVisitor {
			boolean broken;
			long foundValue = NOT_USED_INTEGER;
			StatePath foundPath;
			@Override
			public void visitString( StatePath path, StringStateValue value) {
			    broken = true;
			}
            @Override
			public void visitInteger( StatePath path, IntegerStateValue value)  {
                foundPath = path;
                foundValue = value.getValue();
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

		for( long testInteger : TEST_INTEGERS) {

			IntegerStateValue testValue = new IntegerStateValue( testInteger);

			TestIntegerVisitor testVisitor = new TestIntegerVisitor();

			testValue.acceptVisitor( path, testVisitor);

			assertFalse( "visitor called back on a method it shouldn't have", testVisitor.broken);
			assertTrue( "visitor failed to discover integer value", testVisitor.foundValue == testInteger);
			assertEquals( "visitor failed to return equal StatePath", path, testVisitor.foundPath);
		}
	}

	@Test
	public void testEqualsObject() {

		for( long firstInteger : TEST_INTEGERS) {

			IntegerStateValue firstIsv = new IntegerStateValue( firstInteger);

			for( long secondInteger : TEST_INTEGERS) {

				IntegerStateValue secondIsv = new IntegerStateValue( secondInteger);
				boolean shouldBeEqual;

				shouldBeEqual = firstInteger == secondInteger;

				if( shouldBeEqual) {
					assertEquals( "two IntegerStateValues same creating long, first.equals(second)", firstIsv, secondIsv);
					assertEquals( "two IntegerStateValues same creating long, second.equals(first)", secondIsv, firstIsv);
				} else {
					assertFalse( "two IntegerStateValues different creating long, first.equals(second)", firstIsv.equals( secondIsv));
					assertFalse( "two IntegerStateValues different creating long, second.equals(first)", secondIsv.equals( firstIsv));
				}
			}
		}
	}

	@Test
	public void testIntegerStateValueLongLong() {
		for( long intVal : TEST_INTEGERS) {
                    assertIsEphemeral("creating with integer: " + intVal, new IntegerStateValue(intVal));
                }
	}

	@Test
	public void testIntegerStateValueLongBoolean() {
		for( long intVal : TEST_INTEGERS) {
                    assertIsEphemeral("creating with integer: " + intVal, new IntegerStateValue(intVal, false));
                }
		for( long intVal : TEST_INTEGERS) {
                    assertIsImmortal("creating with integer: " + intVal, new IntegerStateValue(intVal, true));
                }
	}


	@Test
	public void testIntegerStateValueLong() {
		for( long intVal : TEST_INTEGERS) {

			for( long duration : TEST_DURATIONS) {
				IntegerStateValue testVal = new IntegerStateValue( intVal, duration);
				assertIsMortal( "IntegerStateValue \"" + intVal + "\" + " + Long.toString(duration), testVal, duration < 0 ? 0 : duration);
			}

		}
	}

	@Test
	public void testToString() {
		for( long intVal : TEST_INTEGERS) {
			IntegerStateValue testVal = new IntegerStateValue( intVal);

			assertEquals( "toString() failed", Long.toString( intVal), testVal.toString());
		}
	}

	@Test
	public void testGetValue() {
		for( long intVal : TEST_INTEGERS) {
			IntegerStateValue testVal = new IntegerStateValue( intVal);

			assertTrue( "getValue() returned wrong value", intVal == testVal.getValue());
		}
	}

}
