package org.dcache.services.info.base;


public class BooleanStateValue extends StateValue {
	
	private boolean _storage;

	/**
	 * Create a new BooleanStateValue with given value.  This
	 * state value should not expire.
	 * @param value the boolean to store.
	 */
	private BooleanStateValue( boolean value) {
		super();
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
	
	public String toString() {
		return Boolean.toString( _storage);
	}


	public StateValue clone() {		
		BooleanStateValue sv = new BooleanStateValue( _storage);
		sv._creationTime = this._creationTime;
		return sv;
	}

	public String getTypeName() {
		return "boolean";
	}
	

	/**
	 *  Leaf-node specific support for the Visitor pattern.  See StateValue for inherited
	 *  actual implementation and StateVisitor interface for more details. 
	 */
	public void acceptVisitor(StatePath path, StateVisitor visitor) {
		visitor.visitBoolean( path, this);
	}

}
