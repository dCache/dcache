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
		super(false);
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
	
	public boolean shouldTriggerWatcher( StateComponent newValue) {
		if( !(newValue instanceof BooleanStateValue))
			return true;

		BooleanStateValue newBoolValue = (BooleanStateValue) newValue;
		
		return newBoolValue._storage != _storage;
	}

}
