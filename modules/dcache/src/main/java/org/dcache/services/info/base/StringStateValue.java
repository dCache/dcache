/**
 *
 */
package org.dcache.services.info.base;

/**
 * The StringStateValue Class allows String values to be stored within dCache's State.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StringStateValue extends StateValue {

	private final String _storage;
	private static final int NULL_HASH_CODE = 0xdeadbeaf;

	/**
	 * Create a new Ephemeral String StateValue
	 * @param value the String to be stored.
	 */
	public StringStateValue( String value) {
		this( value, false);
	}

	/**
	 * Create a String StateValue that is either immortal or ephermal.
	 * @param value the String value to store
	 * @param isImmortal true if the value is immortal, false otherwise
	 */
	public StringStateValue( String value, boolean isImmortal) {
		super( isImmortal);
		_storage = value;
	}

	/**
	 * Create an expiring String StateValue.
	 * @param value the String to store.
	 * @param duration the duration, in seconds, this should be maintained within dCache state.
	 */
	public StringStateValue( String value, long duration) {
		super( duration);
		_storage = value;
	}

        public String getValue()
        {
            return _storage;
        }

	@Override
	public String toString() {
		return _storage != null ? _storage : "(null)";
	}

	@Override
	public String getTypeName() {
		return "string";
	}


	/**
	 * Needed for the Visitor pattern.  See StateVisitor interface for more details.
	 */
	@Override
	public void acceptVisitor( StatePath path, StateVisitor visitor) {
		visitor.visitString( path, this);
	}

	/**
	 * Override the default hashCode to honour the hashCode() equals() contract.
	 */
	@Override
	public int hashCode() {
		return _storage == null ? NULL_HASH_CODE : _storage.hashCode();
	}

	/**
	 * Override the default equals.
	 */
	@Override
	public boolean equals( Object other) {

		if( !(other instanceof StringStateValue)) {
                    return false;
                }

		StringStateValue otherValue = (StringStateValue) other;

		if( _storage == null) {
                    return otherValue._storage == null;
                }

		return _storage.equals( otherValue._storage);
	}
}
