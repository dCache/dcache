package dmg.cells.nucleus;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Interface encapsulating a cell as seen by cell components.
 *
 * The main difference between the <code>CellEndpoint</code> and the
 * <code>Cell</code> interfaces is the intended client:
 *
 * The <code>Cell</code> interface is an abstraction of a cell as seen
 * by the cell nucleus. It provides methods used by the nucleus to
 * deliver messages to the cell.
 *
 * The <code>CellEndpoint</code> interface is an abstraction of a cell
 * as seen by other components of the cell. It provides methods for
 * sending messages and obtaining information about the cell.
 */
public interface CellEndpoint
{
    enum SendFlag
    {
        /**
         * In case of failure to deliver the message, delivery is transparently
         * retried as long as the timeout has not expired.
         */
        RETRY_ON_NO_ROUTE_TO_CELL
    }

    /**
     * Sends <code>envelope</code>.
     *
     * @param envelope the cell message to be sent.
     * @throws SerializationException if the payload object of this
     *         message is not serializable.
     */
    void sendMessage(CellMessage envelope)
        throws SerializationException;

    /**
     * Sends <code>envelope</code>.
     *
     * The <code>callback</code> argument (which has to be non-null)
     * specifies an object which is informed as soon as an has answer
     * arrived or if the timeout has expired.
     *
     * The callback is run in the supplied executor. The executor may
     * execute the callback inline, but such an executor must only be
     * used if the callback is non-blocking, and the callback should
     * refrain from CPU heavy operations. Care should be taken that
     * the executor isn't blocked by tasks waiting for the callback;
     * such tasks could lead to a deadlock.
     *
     * @param envelope the cell message to be sent.
     * @param callback specifies an object class which will be informed
     *                 as soon as the message arrives.
     * @param executor the executor to run the callback in
     * @param timeout  is the timeout in msec.
     * @param flags    flags affecting how the message is sent
     * @exception SerializationException if the payload object of this
     *            message is not serializable.
     */
    void sendMessage(CellMessage envelope,
                     CellMessageAnswerable callback,
                     Executor executor,
                     long timeout,
                     SendFlag... flags)
        throws SerializationException;

    /**
     * Returns the domain context. The domain context is shared by all
     * cells in a domain.
     */
    Map<String,Object> getDomainContext();
}
