/**
 * 
 */
package org.dcache.services.info.base;

/**
 * Create a new StateUpdate that can be appended.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class AppendableStateUpdate extends StateUpdate {
	
	public AppendableStateUpdate() {
		super();
	}

	/**
	 * Add a new metric update to this StateUpdate object.
	 */
	public void appendUpdate( StatePath path, StateComponent value) {
		super.appendUpdate(path, value);
	}
}
