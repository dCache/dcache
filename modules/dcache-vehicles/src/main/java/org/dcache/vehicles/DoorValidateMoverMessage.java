package org.dcache.vehicles;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import java.io.Serializable;

/**
 * A from a pool to the door to validate validate a mover. The pool provides
 * <tt>moverId</tt>, <tt>pnfsId</tt> and <tt>challenge</tt> to identify itself a door.
 * <p>
 * NOTICE: the message provide a vehicle for moverId, but current mover implementation does not
 * provides such information.
 *
 * @param <T> the type of challenge used by this message
 * @since 2.12
 */
public class DoorValidateMoverMessage<T extends Serializable> extends Message {

    private static final long serialVersionUID = -2105249651572604794L;

    private final T _challenge;
    private final int _moverId;
    private final long _verifier;
    private final PnfsId _pnfsId;

    private boolean _isValid;

    /**
     * Construct a new <tt>DoorValidateMoverMessage</tt> for a given mover. The <tt>verifier</tt>
     * field the to allow client to detect pool restarts.
     *
     * @param moverId   pool specific identifier of the mover or -1 if unknown
     * @param pnfsId    the pnfsid of the file which mover serves
     * @param verifier  pool restart verifier
     * @param challenge an opaque, protocol specific data which identifies the transfer
     */
    public DoorValidateMoverMessage(int moverId, PnfsId pnfsId, long verifier, T challenge) {
        _moverId = moverId;
        _verifier = verifier;
        _challenge = challenge;
        _pnfsId = pnfsId;
    }

    /**
     * Returns true if mover is valid.
     *
     * @return true if mover is valid.
     */
    public boolean isIsValid() {
        return _isValid;
    }

    /**
     * Get <tt>challenge</tt> provided by mover to validate.
     *
     * @return challenge to validate.
     */
    public T getChallenge() {
        return _challenge;
    }

    /**
     * Get moverid which have triggered the validation.
     *
     * @return moverid
     */
    public int getMoverId() {
        return _moverId;
    }

    /**
     * Get verifier to detect pool restarts. Typically pool's startup time.
     *
     * @return pool restart verifier.
     */
    public long getVerifier() {
        return _verifier;
    }

    /**
     * Return <tt>PnfsId</tt> of a file associated with the mover.
     *
     * @return pnfsid of the file
     */
    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public void setIsValid(boolean isValid) {
        _isValid = isValid;
    }

}
