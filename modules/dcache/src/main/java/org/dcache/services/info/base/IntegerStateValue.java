package org.dcache.services.info.base;


/**
 * Extends the abstract StateValue to allow the storage of 64-bit signed numbers
 * within dCache state.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class IntegerStateValue extends StateValue {

	private final long _storage;

	/**
	 * Create a mortal Integer StateValue.
	 * @param value the Integer to store.
	 * @param duration the duration, in seconds, this should be maintained within
	 * dCache state.
	 */
	public IntegerStateValue( long value, long duration) {
		super( duration);
		_storage = value;
	}

	/**
	 * Create an Ephemeral Integer StateValue.
	 * @param value the Integer to store.
	 */
	public IntegerStateValue( long value) {
		this( value, false);
	}


	/**
	 * Create a Integer StateValue that is either immortal or ephemeral.
	 * @param value the numerical value to store
	 * @param isImmortal whether this metric is immortal. If false, then an ephemeral value is
	 * created, equivalent to IntegerStateValue( value).
	 */
	public IntegerStateValue( long value, boolean isImmortal) {
		super( isImmortal);
		_storage = value;
	}

	/**
	 * Return a string representation.
	 */
	@Override
	public String toString() {
		return Long.toString( _storage);
	}

	@Override
	public String getTypeName() {
		return "integer";
	}

	public long getValue() {
		return _storage;
	}

	/**
	 *  Leaf-node specific support for the Visitor pattern.  See StateValue for inherited
	 *  actual implementation and StateVisitor interface for more details.
	 */
	@Override
	public void acceptVisitor( StatePath path, StateVisitor visitor) {
		visitor.visitInteger( path, this);
	}

	/**
	 *  Override the default hashCode() method, to honour the hashCode() / equals() contract.
	 */
	@Override
	public int hashCode() {
		return (int)_storage;
	}


	/**
	 *  Override the default equals() method.
	 */
	@Override
	public boolean equals( Object other) {

		if( !( other instanceof IntegerStateValue)) {
                    return false;
                }

		IntegerStateValue otherValue = (IntegerStateValue) other;

		return _storage == otherValue._storage;
	}


}
