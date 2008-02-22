package org.dcache.services.info.gathers;


/**
 * Classes that implement CellMessageHandler interface can process the reply from a CellMessage
 * request.  This interface is needed for CellMessages that are not vehicles (i.e., the payload
 * object is not a Message subclass.
 * 
 * @deprecated this interface is deprecated.  Classes that implements this interface should
 * use MessageHandler interface instead, which implies a different messaging system.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface CellMessageHandler {

	/**
	 * Process an incoming CellMessage payload
	 * @param msgPayload the CellMessage's payload
	 * @param delay the delay, in seconds, until the next message.
	 */
	void process( Object msgPayload, long delay);
}
