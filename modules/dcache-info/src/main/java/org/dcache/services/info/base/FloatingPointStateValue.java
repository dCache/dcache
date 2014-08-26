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
		this( value, false);
	}


	/**
	 * Create a new FloatingPoint StateValue that is either Immortal or Ephemeral.
	 * @param value the floating-point number to store.
	 * @param isImmortal true if this is immortal, otherwise ephemeral.
	 */
	public FloatingPointStateValue( double value, boolean isImmortal) {
		super( isImmortal);
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

	@Override
	public String getTypeName() {
		return "float";
	}

	@Override
	public String toString() {
		return Double.toString( _storage);
	}

        public double getValue() {
            return _storage;
        }

	/**
	 *  Leaf-node specific support for the Visitor pattern.  See StateValue for inherited
	 *  actual implementation and StateVisitor interface for more details.
	 */
	@Override
	public void acceptVisitor(StatePath path, StateVisitor visitor) {
		visitor.visitFloatingPoint( path, this);
	}

	/**
	 *  Override the default hashCode() method, to honour the hashCode() / equals() contract.
	 */
	@Override
	public int hashCode() {
		// TODO: do something better here.
		Float floatVal = new Float( _storage);
		return floatVal.hashCode();
	}


	/**
	 *  Override the default equals() method.
	 */
	@Override
	public boolean equals( Object other) {

		if( !( other instanceof FloatingPointStateValue)) {
                    return false;
                }

		FloatingPointStateValue otherValue = (FloatingPointStateValue) other;

		return _storage == otherValue._storage;
	}


}
