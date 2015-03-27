package dmg.cells.nucleus;

import java.io.Serializable;

/**
 * A {@code Reply} represents a reply to a message.
 */
public interface Reply extends Serializable
{
    /**
     * When a message handler returns a {@code Reply}, its deliver method is called to allow
     * the reply to be delivered. An implementation may send the reply immediately or
     * after the method has returned.
     *
     * An implementation should revert the direction of the envelope before sending the
     * reply.
     *
     * @param endpoint The CellEndpoint on which to deliver the reply.
     * @param envelope The original CellMessage to which this is a reply.
     */
    void deliver(CellEndpoint endpoint, CellMessage envelope);
}
