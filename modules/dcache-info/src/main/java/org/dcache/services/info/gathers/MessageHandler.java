package org.dcache.services.info.gathers;

import diskCacheV111.vehicles.Message;

/**
 * All classes that (might) handle an incoming message must implement this
 * interface.  Handle, in this context, means updates the dCache State with
 * fresh data.
 *
 * Implementations must not assume a particular order in which the MessageHandler
 * objects are called.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface MessageHandler
{
    /**
     * Attempt to update dCache state, based on the incoming message.
     *
     * @param msg the incoming message payload
     * @param delay the expected time, in seconds, until this data is refreshed.
     * @return true if the message was successfully handled, false otherwise.
     */
    boolean handleMessage(Message messagePayload, long delay);
}
