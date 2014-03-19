package dmg.cells.nucleus;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.dcache.util.Args;

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
   /**
    * Sends <code>envelope</code>.
    *
    * @param envelope the cell message to be sent.
    * @throws SerializationException if the payload object of this
    *         message is not serializable.
    * @throws NoRouteToCellException if the destination could not be
    *         reached.
    */
    void sendMessage(CellMessage envelope)
        throws SerializationException,
               NoRouteToCellException;

    /**
    * Sends <code>envelope</code>. The <code>callback</code> argument
    * (which has to be non-null) allows to specify an object which is
    * informed as soon as an has answer arrived or if the timeout has
    * expired.
    *
    * @param envelope the cell message to be sent.
    * @param callback specifies an object class which will be informed
    *                 as soon as the message arrives.
    * @param timeout  is the timeout in msec.
    * @exception SerializationException if the payload object of this
    *            message is not serializable.
    */
    void sendMessage(CellMessage envelope,
                     CellMessageAnswerable callback,
                     long timeout)
        throws SerializationException;

    /**
     * Sends <code>envelope</code>. The <code>callback</code> argument
     * (which must be non-null) specifies an object which is informed as
     * soon as an has answer arrived or if the timeout has expired.
     *
     * In case of failure to deliver the message, delivery is transparently
     * retried as long as the timeout has not expired.
     *
     * @param envelope the cell message to be sent.
     * @param callback specifies an object class which will be informed
     *                 as soon as the message arrives.
     * @param timeout  is the timeout in msec.
     * @exception SerializationException if the payload object of this
     *            message is not serializable.
     */
    void sendMessageWithRetryOnNoRouteToCell(CellMessage envelope,
                                             CellMessageAnswerable callback,
                                             long timeout)
            throws SerializationException;

    /**
     * Provides information about the host cell.
     *
     * Depending on the cell, a subclass of CellInfo with additional
     * information may be returned instead.
     *
     * @return The cell information encapsulated in a CellInfo object.
     */
    CellInfo getCellInfo();

    /**
     * Returns the domain context. The domain context is shared by all
     * cells in a domain.
     */
    Map<String,Object> getDomainContext();

    /**
     * Returns the cell command line arguments provided when the cell
     * was created.
     */
    Args getArgs();
}
