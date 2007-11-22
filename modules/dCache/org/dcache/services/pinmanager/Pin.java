package org.dcache.services.pinmanager;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Collection;
import java.util.ArrayList;
import java.io.NotSerializableException;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PinManagerMessage;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsId;

/**
 * The Pin class acts as a container for pin requests. Most of the
 * logic for handling pin and unpin requests and expiring requests is
 * handled by this class.
 *
 * Most of this class is synchronized, since the state machine can
 * only handle a single event at a time.
 */
public class Pin extends SMCDriver
{
    /** PNFS ID of the pinned file. */
    private final PnfsId _pnfsId;

    /** All requests to pin the file. */
    private final Collection<PinRequest> _requests =
        new ArrayList<PinRequest>();

    /** The pin manager which this pin belongs to. */
    private final PinManager _manager;

    /** The state machine containing all the logic. */
    private final PinContext _fsm;

    /** Storage info of the pinned file. Null if not yet available. */
    private StorageInfo _storageInfo;

    /** Pinner, unpinner or null if neither is running. */
    private Object _handler;

    /**
     * There is a timer task for each request to handle expiration.
     * This is the timer driving those tasks.
     */
    private static Timer _timer = new Timer(true);

    public Pin(PinManager manager, PnfsId pnfsId)
    {
        _manager = manager;
        _pnfsId = pnfsId;
        _fsm = new PinContext(this);
    }

    /**
     * Part of the SMCDriver interface. Returns the state machine of
     * the pin.
     */
    protected PinContext getContext()
    {
        return _fsm;
    }

    protected void debug(String s)
    {
        _manager.debug(s);
    }

    protected void info(String s)
    {
        _manager.info(s);
    }

    protected void warn(String s)
    {
        _manager.warn(s);
    }

    protected void error(String s)
    {
        _manager.error(s);
    }

    protected void fatal(String s)
    {
        _manager.fatal(s);
    }

    /**
     * Handles messages from cell layers. Such messages are translated
     * into events and fed into the state machine.
     */
    synchronized public void messageArrived(CellMessage envelope,
                                            PinManagerMessage msg)
    {
        transition("messageArrived", envelope, msg);
    }

    /**
     * Sets the storage info.
     *
     * If possible, the storage info is taken from the first
     * request. Otherwise the pinner will fetch it from PNFS.
     */
    synchronized public void setStorageInfo(StorageInfo info)
    {
        _storageInfo = info;
    }

    synchronized public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("pin of : ").append(_pnfsId).append('\n');
        sb.append(" state : ").append(_fsm.getState()).append('\n');
        sb.append(" handler : ").append(_handler).append('\n');
        sb.append(" requests :\n");
        if (isEmpty()) {
            sb.append("    none\n");
        } else {
            for (PinRequest request : _requests) {
                sb.append("  ").append(request).append('\n');
            }
        }
        return sb.toString();
    }

    synchronized public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    /**
     * Adds a request recovered from the database.
     */
    synchronized public void recover(PinRequest request)
    {
        _fsm.recover(request);
    }

    /**
     * Pin the file.
     *
     * This is for pinning via the admin interface. These requests are
     * not confirmed.
     */
    synchronized public void adminPin(long lifetime)
    {
        _fsm.adminPin(lifetime);
    }

    /**
     * Release a pin request.
     *
     * This is for unpinning via the admin interface. These requests
     * are not confirmed.
     *
     * Returns true if the request is found, false otherwise.
     */
    synchronized public boolean adminUnpin(long pinId)
    {
        PinRequest request = getRequest(pinId);
        if (request == null)
            return false;
        _fsm.adminUnpin(request);
        return true;
    }

    /**
     * Triggers expiration of a request.
     */
    synchronized void timeout(PinRequest request)
    {
        _fsm.timeout(request);
    }

    /**
     * Callback from pinner.
     */
    synchronized void pinSucceeded()
    {
        info("Successfully pinned " + _pnfsId);
        _handler = null;
        _fsm.pinSucceeded();
    }

    /**
     * Callback from pinner.
     */
    synchronized void pinFailed(Object reason)
    {
        error("Failed to pin " + _pnfsId + ": " + reason);
        _handler = null;
        _fsm.pinFailed();
    }

    /**
     * Callback from unpinner.
     */
    synchronized void unpinSucceeded()
    {
        info("Successfully unpinned " + _pnfsId);
        _handler = null;
        _fsm.unpinSucceeded();
    }

    /**
     * Callback from unpinner.
     */
    synchronized void unpinFailed(Object reason)
    {
        error("Failed to unpin " + _pnfsId + ": " + reason);
        _handler = null;
        _fsm.unpinFailed();
    }

    /**
     * Returns a new pin request with the given lifetime and client
     * ID. The request is not yet added to the pin.
     *
     * @see add
     */
    PinRequest createRequest(long lifetime, long clientId)
    {
        debug("pin pnfsId=" + _pnfsId + " lifetime=" + lifetime);

        long max = _manager.getMaxPinDuration();
        if (lifetime > max) {
            lifetime = max;
            warn("Pin lifetime exceeded maxPinDuration, new lifetime set to "
                 + lifetime);
        }

        long expiration = System.currentTimeMillis() + lifetime;
        return _manager.getDatabase().createRequest(_pnfsId, expiration, clientId);
    }

    /**
     * Returns a new pin request matching the given pin message.  The
     * request is not yet added to the pin.
     *
     * @see add
     */
    PinRequest createRequest(CellMessage envelope, PinManagerPinMessage message)
    {
        long lifetime = message.getLifetime();
        long clientId = message.getRequestId();
        PinRequest request = createRequest(lifetime, clientId);
        request.setCellMessage(envelope);
        return request;
    }

    /**
     * Adds a pin request to the pin.
     *
     * If the request contains storage information and storage
     * information is not yet known for the pin, then this information
     * is extracted from the request.
     *
     * A new expiration timer is started.
     *
     * @return The request.
     * @see remove
     */
    synchronized PinRequest add(final PinRequest request)
    {
        if (_storageInfo == null && request.getRequest() != null ) {
            _storageInfo = request.getRequest().getStorageInfo();
        }

        _requests.add(request);

        TimerTask task = new TimerTask() {
                public void run() {
                    /* Since the lifetime can be extended, the
                     * remaining lifetime may be non-zero at this
                     * point. If it is, we reschedule the task.
                     */
                    long lifetime = request.getRemainingLifetime();
                    if (lifetime > 0) {
                        _timer.schedule(this, lifetime);
                    } else {
                        _fsm.timeout(request);
                    }
                }
            };
        request.setTimer(task);
        _timer.schedule(task, request.getRemainingLifetime());

        return request;
    }

    /**
     * Removes a request from this pin. This involves unregistering
     * any timers and making sure the request is deleted from the
     * database.
     *
     * @see add
     */
    synchronized void remove(PinRequest request)
    {
        TimerTask task = request.getTimer();
        if (task != null)
            task.cancel();

        _requests.remove(request);
        _manager.getDatabase().deleteRequest(request);
    }


    /**
     * Return the request with the given pin ID, or null if such a
     * request does not exist.
     */
    synchronized PinRequest getRequest(long id)
    {
        for (PinRequest request : _requests) {
            if (request.getPinRequestId() == id) {
                return request;
            }
        }
        return null;
    }

    /**
     * Return the request with the given pin ID, or null if such a
     * request does not exist.
     */
    PinRequest getRequest(String id)
    {
        try {
            return getRequest(Long.parseLong(id));
        } catch (NumberFormatException e) {
            /* Since the id is not a number, it will not match any of
             * our requests. Thus by definition this particular
             * request does not exist.
             */
        }
        return null;
    }

    /**
     * Return the request with the pin ID provided in the message, or
     * null if such a request does not exist.
     */
    PinRequest getRequest(PinManagerUnpinMessage msg)
    {
        return getRequest(msg.getPinId());
    }

    /**
     * Return the request with the pin ID provided in the message, or
     * null if such a request does not exist.
     */
    PinRequest getRequest(PinManagerExtendLifetimeMessage msg)
    {
        return getRequest(msg.getPinId());
    }

    /**
     * Sends a failure response for the given message.
     *
     * A reply is only send if required by the message.
     */
    void returnFailure(CellMessage envelope, int rc, Object reason)
    {
        error("Returning failure: " + reason);

        if (reason != null && !(reason instanceof java.io.Serializable)) {
            reason = reason.toString();
        }

        Message m = (Message)envelope.getMessageObject();
        if (m.getReplyRequired()) {
            try {
                m.setFailed(rc, reason);
                envelope.revertDirection();
                _manager.sendMessage(envelope);
            } catch (NoRouteToCellException e) {
                error("Cannot send failure response: " + e.getMessage());
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected", e);
            }
        }
    }

    /**
     * Returns an success acknowledgement for the given message.
     *
     * A reply is only send if required by the message.
     */
    void returnSuccess(CellMessage envelope)
    {
        Message m = (Message)envelope.getMessageObject();
        if (m.getReplyRequired()) {
            try {
                m.setSucceeded();
                envelope.revertDirection();
                _manager.sendMessage(envelope);
            } catch (NoRouteToCellException e) {
                error("Can not send response: " + e.getMessage());
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected", e);
            }
        }
    }

    /**
     * Sends a positive response back to the requester, if any.
     */
    void confirm(PinRequest request)
    {
        CellMessage envelope = request.getCellMessage();
        if (envelope != null) {
            returnSuccess(envelope);

            /* To reduce memory consumption, we remove the cell
             * message from the request object.
             */
            request.setCellMessage(null);
        }
    }

    /**
     * Sends a negative response back to the requester, if any.
     */
    void fail(PinRequest request)
    {
        CellMessage envelope = request.getCellMessage();
        if (envelope != null) {
            returnFailure(envelope, 1, "Pinning failed");

            /* To reduce memory consumption, we remove the cell
             * message from the request object.
             */
            request.setCellMessage(null);
        }
    }

    /**
     * Confirms all requests.
     *
     * @see confirm
     */
    synchronized void confirmAll()
    {
        for (PinRequest request : _requests) {
            confirm(request);
        }
    }

    /**
     * Fails all requests and removes then from the pin.
     *
     * @see fail, remove
     */
    synchronized void failAndRemoveAll()
    {
        /* Since remove alters the _requests collection, we need to
         * make a copy of it to avoid ConcurrentModificationException.
         */
        for (PinRequest request : new ArrayList<PinRequest>(_requests)) {
            fail(request);
            remove(request);
        }
    }

    /**
     * Returns true iff there are no requests.
     */
    synchronized boolean isEmpty()
    {
        return _requests.isEmpty();
    }

    /**
     * Returns true iff <code>request</code> is the only request.
     */
    synchronized boolean isLast(PinRequest request)
    {
        return _requests.size() == 1 && _requests.contains(request);
    }

    /**
     * Extends the lifetime of the request. The request is updated in
     * the database. The request lifetime can only be extended. If the
     * new lifetime is shorter than the remaining time, then nothing
     * is changed.
     */
    void extendLifetime(PinRequest request, long newLifetime)
    {
        long max = _manager.getMaxPinDuration();
        if (newLifetime > max) {
            newLifetime = max;
            warn("Pin duration exceeded maxPinDuration, lifetime set to "
                 + newLifetime);
        }

        if (request.getRemainingLifetime() < newLifetime) {
            request.setExpiration(System.currentTimeMillis() + newLifetime);
            _manager.getDatabase().updateRequest(request);

            /* The timer associated with the request checks the
             * expiration time before expiring the request. If it has
             * not expired yet it reschedules itself. Therefore we do
             * not need to update the timer here.
             */
        }
    }

    /**
     * Starts an asynchronous pin operation.
     */
    synchronized void startPinner()
    {
        _handler = new Pinner(_manager, _pnfsId, _storageInfo, this);
    }

    /**
     * Starts an asynchronous unpin operation.
     */
    synchronized void startUnpinner()
    {
        _handler = new Unpinner(_manager, _pnfsId, this);
    }

    /**
     * Removes this pin object from the pin manager.
     */
    synchronized void unregister()
    {
        if (!_requests.isEmpty())
            throw new IllegalStateException("Cannot unregister pin with requests");

        _manager.removePin(_pnfsId);
    }
}
