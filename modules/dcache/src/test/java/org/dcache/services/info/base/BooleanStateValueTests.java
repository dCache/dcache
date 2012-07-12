package org.dcache.services.info.base;


import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

public class BooleanStateValueTests extends InfoBaseTestHelper {

	/**
	 * A simple visitor to test that acceptVisitor works as expected.
	 */
	class TestBooleanVisitor implements StateVisitor {
		boolean _visitCalledWrongMethod;
		BooleanStateValue _value;
		StatePath _visitPath;

		// Visit a BooleanStateValue: we assert that this hasn't happened before.
		@Override
                public void visitBoolean( StatePath path, BooleanStateValue value) {
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
		public void visitFloatingPoint( StatePath path, FloatingPointStateValue value) {
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
		int trueHash = new BooleanStateValue( true).hashCode();
		int falseHash = new BooleanStateValue( false).hashCode();

		assertTrue( "Two hash values are identical", trueHash != falseHash);
	}


	@Test
	public void testGetTypeName() {
		String typeName = new BooleanStateValue( true).getTypeName();

		assertEquals( "Boolean typeName", "boolean", typeName);
	}


	@Test
	public void testAcceptVisitorStatePathStateVisitor() {

		BooleanStateValue booleanStateValue = new BooleanStateValue( true);
		TestBooleanVisitor visitor = new TestBooleanVisitor();
		StatePath myPath = StatePath.parsePath("first.second");

		booleanStateValue.acceptVisitor( myPath, visitor);

		assertFalse( "visitor called wrong methods", visitor._visitCalledWrongMethod);
		assertEquals( "visit path not the same", visitor._visitPath, myPath);
		assertEquals( "visit value not the same", visitor._value, booleanStateValue);
	}


	@Test
	public void testBooleanStateValueBoolean() {
		assertIsEphemeral( "Not as expected", new BooleanStateValue( false));
		assertIsEphemeral( "Not as expected", new BooleanStateValue( true));
	}

	@Test
	public void testBooleanStateValueBooleanBoolean() {
		assertIsEphemeral( "Not as expected", new BooleanStateValue( false, false));
		assertIsEphemeral( "Not as expected", new BooleanStateValue( true, false));

		assertIsImmortal( "Not as expected", new BooleanStateValue( false, true));
		assertIsImmortal( "Not as expected", new BooleanStateValue( true, true));
}

	@Test
	public void testBooleanStateValueBooleanLong() {

		for( long duration = -1; duration < 3; duration++) {
			assertIsMortal( "boolean: false + " + Long.toString(duration), new BooleanStateValue( false, duration), duration < 0 ? 0 : duration);
			assertIsMortal( "boolean: true + " + Long.toString(duration), new BooleanStateValue( true, duration), duration < 0 ? 0 : duration);
		}
	}

}
