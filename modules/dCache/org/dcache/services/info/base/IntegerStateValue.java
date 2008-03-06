package org.dcache.services.info.base;


/**
 * Extends the abstract StateValue to allow the storage of 64-bit signed numbers
 * within dCache state.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class IntegerStateValue extends StateValue {
	
	private static final long DUMMY_VALUE = 0;

	private long _storage;

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
	 * Create an Emphemeral Integer StateValue.
	 * @param value the Integer to store.
	 */
	public IntegerStateValue( long value) {
		super( false);
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
		IntegerStateValue sv;
		
		if( this.isEphemeral())
			sv = new IntegerStateValue( _storage);
		else {
			sv = new IntegerStateValue( _storage, DUMMY_VALUE);
			sv._expiryTime = this._expiryTime;
		}
		return sv;
	}
	
	public String getTypeName() {
		return "integer";
	}
	
	/**
	 * Return the value stored within this IntegerStateValue
	 * @return
	 */
	public long getValue() {
		return _storage;
	}

	/**
	 *  Leaf-node specific support for the Visitor pattern.  See StateValue for inherited
	 *  actual implementation and StateVisitor interface for more details. 
	 */
	public void acceptVisitor( StatePath path, StateVisitor visitor) {
		visitor.visitInteger( path, this);
	}
	
	public boolean shouldTriggerWatcher( StateComponent newValue) {
		if( !(newValue instanceof IntegerStateValue))
			return true;

		IntegerStateValue newIntValue = (IntegerStateValue) newValue;
		
		return newIntValue._storage != _storage;
	}

}
