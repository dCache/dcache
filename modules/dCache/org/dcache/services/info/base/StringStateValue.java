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

	/**
	 * Create a new Ephemeral String StateValue
	 * @param value the String to be stored.
	 */
	public StringStateValue( String value) {
		super( false);
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
	
	public String toString() {
		return _storage;
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
	
	/**
	 * Whether the differences between supplied StateComponent and ourselves is sufficient
	 * that it should trigger a StateWatcher.
	 */
	public boolean shouldTriggerWatcher( StateComponent newValue) {
		if( !(newValue instanceof StringStateValue))
			return true;
		StringStateValue newStringValue = (StringStateValue) newValue;
		
		return !newStringValue._storage.equals( _storage);
	}

}
