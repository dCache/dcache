package org.dcache.services.info.base;


public class IntegerStateValue extends StateValue {

	private long _storage;

	/**
	 * Create a static (non-expiring) String StateValue.
	 * @param value the String to store.
	 */
	private IntegerStateValue( long value) {
		super();
		_storage = value;
	}
	
	/**
	 * Create an expiring String StateValue.
	 * @param value the String to store.
	 * @param duration the duration, in seconds, this should be maintained within
	 * dCache state.
	 */
	public IntegerStateValue( long value, long duration) {
		super( duration);
		_storage = value;
	}	
	
	/**
	 * Return a string representation.
	 */
	public String toString() {
		return Long.toString( _storage);
	}

	/**
	 * Return a new StateValue.
	 */
	public StateValue clone() {		
		IntegerStateValue sv = new IntegerStateValue( _storage);
		sv._expiryTime = this._expiryTime;
		return sv;
	}
	
	public String getTypeName() {
		return "integer";
	}

	/**
	 *  Leaf-node specific support for the Visitor pattern.  See StateValue for inherited
	 *  actual implementation and StateVisitor interface for more details. 
	 */
	public void acceptVisitor( StatePath path, StateVisitor visitor) {
		visitor.visitInteger( path, this);
	}
}
