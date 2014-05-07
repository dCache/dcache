package org.dcache.services.info.base;

/**
 * Extends the abstract StateValue class to allow storage of boolean values
 * within dCache state.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class BooleanStateValue extends StateValue {

	private final boolean _storage;

	/**
	 * Create a new Ephemeral Boolean StateValue.
	 */
	public BooleanStateValue( boolean value) {
		this( value, false);
	}


	/**
	 * Create a new Boolean StateValue that is either Immortal or Ephemeral.
	 */
	public BooleanStateValue( boolean value, boolean isImmortal) {
		super( isImmortal);
		_storage = value;
	}

	/**
	 * Create a new BooleanStateValue with given value.  This
	 * StateValue should expire after a certain time has elapsed.
	 * @param value the value to store
	 * @param duration the lifetime of this metric, in seconds.
	 */
	public BooleanStateValue( boolean value, long duration) {
		super( duration);
		_storage = value;
	}

	@Override
	public String toString() {
		return Boolean.toString( _storage);
	}

        public boolean getValue() {
            return _storage;
        }

	@Override
	public String getTypeName() {
		return "boolean";
	}


	/**
	 *  Leaf-node specific support for the Visitor pattern.  See StateValue for inherited
	 *  actual implementation and StateVisitor interface for more details.
	 */
	@Override
	public void acceptVisitor(StatePath path, StateVisitor visitor) {
		visitor.visitBoolean( path, this);
	}

	/**
	 *  Override the default hashCode() method, to honour the hashCode() / equals() contract.
	 */
	@Override
	public int hashCode() {
		return _storage ? 1 : 0;
	}


	/**
	 *  Override the default equals() method.
	 */
	@Override
	public boolean equals( Object other) {

		if( !( other instanceof BooleanStateValue)) {
                    return false;
                }

		BooleanStateValue otherValue = (BooleanStateValue) other;

		return _storage == otherValue._storage;
	}

}
