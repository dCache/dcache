package org.dcache.services.info.gathers;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;

/**
 * A Class implementing MessageSender provides both the mechanism to send
 * messages to dCache that request fresh information and do sufficient
 * preparation work so that, when the info cell receives a reply message, it
 * can be processed correctly. This is likely achieved using a
 * {@link MessageMetadataRepository}
 */
public interface MessageSender {

    /**
     * Send an instance of Message to the Cell identified by the provided
     * CellPath. The time-to-live (in seconds) for any resulting metrics is
     * recorded.
     *
     * @param ttl
     *            how long, in seconds, resulting metrics should last
     * @param path
     *            the destination for this request
     * @param message
     *            the vehicle to send
     */
    void sendMessage(long ttl, CellPath path, Message message);

    /**
     * Send some arbitrary CellMessage (which includes the payload and the
     * target Cell). The time-to-live (in seconds) for any resulting metrics
     * is recorded.
     * <p>
     *
     * The reply message (if any) will be handled by the object implementing
     * CellMessageAnswerable. If the handler is null then we assume this is a
     * one-shot message.
     *
     * @param ttl
     *            how long, in seconds, resulting metrics should last
     * @param handler
     *            the object that is to receive reply message
     * @param envelope
     *            the complete message envelope to send
     */
    void sendMessage(long ttl, CellMessageAnswerable handler,
            CellMessage envelope);

    /**
     * Send a message that has a String payload to the Cell identified by the
     * provided CellPath. The time-to-live (in seconds) for any resulting
     * metrics is recorded.
     * <p>
     *
     * This method uses the cell's shell to obtain information. Although not
     * yet deprecated, new code shouldn't use this method but, instead, build
     * a vehicle to send to the cell.
     *
     * @param ttl
     *            how long, in seconds, resulting metrics should last
     * @param handler
     *            the object that is to receive reply message
     * @param path
     *            the destination for this request
     * @param requestString
     *            the String sent to the cell's shell
     */
    void sendMessage(long ttl, CellMessageAnswerable handler, CellPath path,
            String requestString);
}
