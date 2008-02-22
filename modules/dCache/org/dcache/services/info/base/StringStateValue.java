/**
 * 
 */
package org.dcache.services.info.base;

/**
 * A String value stored within dCache's State.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StringStateValue extends StateValue {
	
	private String _storage;

	private StringStateValue() {
		super();
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
	
	public String toString() {
		return _storage;
	}
	
	public StateValue clone() {
		StringStateValue newVal = new StringStateValue();
		newVal._storage = this._storage;
		newVal._expiryTime = this._expiryTime;
		return newVal;
	}
	
	public String getTypeName() {
		return "string";
	}

	
	/**
	 * Needed for the Visitor pattern.  See StateVisitor interface for more details. 
	 */
	public void acceptVisitor( StatePath path, StateVisitor visitor) {
		visitor.visitString( path, this);
	}
}
