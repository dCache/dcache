package org.dcache.services.info.base;


import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

public class FloatingPointStateValueTests extends InfoBaseTestHelper {

	private static final double TEST_FLOATS[] = { 0, 0.01, 1, 10, 100};

	/**
	 * A simple visitor to test that acceptVisitor works as expected.
	 * @author Paul Millar <paul.millar@desy.de>
	 */
	class TestFloatVisitor implements StateVisitor {
		boolean _visitCalledWrongMethod;
		FloatingPointStateValue _value;
		StatePath _visitPath;

		public void visitFloatingPoint( StatePath path, FloatingPointStateValue value) {

			assertNull( "Stored _value not null", _value);

			_value = value;

			assertNull( "Stored _visitPath not null", _visitPath);

			_visitPath = path;
		}

		@Override
		public void visitString( StatePath path, StringStateValue value) {
		    _visitCalledWrongMethod = true;
		}
        @Override
		public void visitInteger( StatePath path, IntegerStateValue value) {
            _visitCalledWrongMethod = true;
        }
        @Override
		public void visitBoolean( StatePath path, BooleanStateValue value) {
            _visitCalledWrongMethod = true;
        }
        @Override
		public void visitCompositePreDescend( StatePath path, Map<String,String> metadata) {
            _visitCalledWrongMethod = true;
        }
        @Override
		public void visitCompositePostDescend( StatePath path, Map<String,String> metadata) {
            _visitCalledWrongMethod = true;
        }

        @Override
        public boolean isVisitable( StatePath path) {
            return true;
        }
	}


	@Test
	public void testHashCode() {
		Set<Integer> seenHashCodes = new TreeSet<Integer>();

		for( double testFloat : TEST_FLOATS) {
			FloatingPointStateValue testValue = new FloatingPointStateValue( testFloat);

			assertFalse( "hashCode for " + testFloat + " repeated",
					hashAlreadySeen( seenHashCodes, testValue.hashCode()));

			FloatingPointStateValue testValue2 = new FloatingPointStateValue( testFloat);

			assertTrue( "hashCode for " + testFloat + " not found again",
					hashAlreadySeen( seenHashCodes, testValue2.hashCode()));
		}
	}

	@Test
	public void testGetTypeName() {
		assertEquals( "Unexpected type name", "float", new FloatingPointStateValue( TEST_FLOATS[0]).getTypeName());
	}

	@Test
	public void testAcceptVisitorStatePathStateVisitor() {

		FloatingPointStateValue floatStateValue = new FloatingPointStateValue( TEST_FLOATS[0]);
		TestFloatVisitor visitor = new TestFloatVisitor();
		StatePath myPath = StatePath.parsePath("first.second");

		floatStateValue.acceptVisitor( myPath, visitor);

		assertFalse( "visitor called wrong methods", visitor._visitCalledWrongMethod);
		assertEquals( "visit path not the same", visitor._visitPath, myPath);
		assertEquals( "visit value not the same", visitor._value, floatStateValue);
	}

	@Test
	public void testFloatingPointStateValueDouble() {
		for( double floatVal : TEST_FLOATS) {
                    assertIsEphemeral("creating with " + floatVal, new FloatingPointStateValue(floatVal));
                }
	}

	@Test
	public void testFloatingPointStateValueDoubleBoolean() {
		for( double floatVal : TEST_FLOATS) {
                    assertIsEphemeral("creating with " + floatVal, new FloatingPointStateValue(floatVal, false));
                }
		for( double floatVal : TEST_FLOATS) {
                    assertIsImmortal("creating with " + floatVal, new FloatingPointStateValue(floatVal, true));
                }
	}

	@Test
	public void testFloatingPointStateValueDoubleLong() {

		for( long duration = -1; duration < 3; duration++) {
			for( double floatVal : TEST_FLOATS) {
                            assertIsMortal("float: \"" + floatVal + "\" + " + Long
                                    .toString(duration), new FloatingPointStateValue(floatVal, duration), duration < 0 ? 0 : duration);
                        }
		}
	}
}
