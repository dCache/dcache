package org.dcache.services.info.base;


/**
 * Extends the generic StateValue to implement storing Floating-point numbers
 * within the dCache state tree.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class FloatingPointStateValue extends StateValue {
	
	private double _storage;

	/**
	 * Create a new FloatingPointStateValue that will store
	 * a floating-point number.  This metric has no life-time.
	 * @param value the value to be stored.
	 */
	private FloatingPointStateValue( double value) {
		super();
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

	public StateValue clone() {
		FloatingPointStateValue sv = new FloatingPointStateValue( _storage);
		sv._expiryTime = this._expiryTime;
		return sv;
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

}
