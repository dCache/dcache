package org.dcache.services.info.conduits;

/**
 * A conduit is the general concept of providing read-only access to the
 * InfoBase: the current knowledge of dCache's state.
 *
 * @author Paul Millar
 */
public interface Conduit
{
    /**
     *  A method that informs a conduit that it should start providing access to
     *  its state.
     */
    void enable();

    /**
     *  A method indicating a conduit should cease all activity.  Once
     *  the <code>stop()</code> method is called.  A conduit should release
     *  resources when told to stop().
     */
    void disable();

    /**
     * Whether the Conduit has been started.
     * @return true if the conduit has been started,
     */
    boolean isEnabled();

    /**
     * Return a single line of text, describing the conduit.
     */
    String getInfo();
}
