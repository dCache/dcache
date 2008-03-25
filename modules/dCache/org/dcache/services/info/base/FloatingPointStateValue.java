package org.dcache.services.info.base;


/**
 * Extends the abstract StateValue to allow storing Floating-point numbers
 * within the dCache state tree.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class FloatingPointStateValue extends StateValue {
	
	private final double _storage;

	/**
	 * Create a new Ephemeral FloatingPoint StateValue that will store
	 * a floating-point number.  This metric has no life-time.
	 * @param value the value to be stored.
	 */
	public FloatingPointStateValue( double value) {
		super( false);
		_storage = value;
	}
	
	/**
	 * Create a new FloatingPointStateValue that will store
	 * a floating-point number within the dCache state tree.
	 * This metric will last for the specified duration before
	 * being flushed from the state.
	 * @param value the value to be stored
	 * @param duration the lifetime, in seconds, for this metric.
	 */
	public FloatingPointStateValue( double value, long duration) {
		super( duration);
		_storage = value;
	}

	public String getTypeName() {
		return "float";
	}
	
	public String toString() {
		return Double.toString( _storage);
	}
	
	/**
	 *  Leaf-node specific support for the Visitor pattern.  See StateValue for inherited
	 *  actual implementation and StateVisitor interface for more details. 
	 */
	public void acceptVisitor(StatePath path, StateVisitor visitor) {
		visitor.visitFloatingPoint( path, this);
	}

	public boolean shouldTriggerWatcher( StateComponent newValue) {
		if( !(newValue instanceof FloatingPointStateValue))
			return true;

		FloatingPointStateValue newFPValue = (FloatingPointStateValue) newValue;
		
		return newFPValue._storage != _storage;
	}

}
