package org.dcache.pinmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.Date;
import java.util.Optional;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.cells.MessageReply;
import org.dcache.pinmanager.model.Pin;
import org.dcache.vehicles.FileAttributes;

public class PinTask
{
    private static final Logger _log = LoggerFactory.getLogger(PinTask.class);

    private final PinManagerPinMessage _request;
    private final Optional<MessageReply<PinManagerPinMessage>> _reply;
    private Pin _pin;
    private PoolMgrSelectReadPoolMsg.Context _readPoolSelectionContext;

    public PinTask(PinManagerPinMessage request, MessageReply<PinManagerPinMessage> reply, Pin pin)
    {
        _request = request;
        _reply = request.isReplyWhenStarted() ? Optional.empty() : Optional.of(reply);
        _pin = pin;
    }

    public Pin getPin()
    {
        return _pin;
    }

    public void setPin(Pin pin)
    {
        _pin = pin;
    }

    public boolean isValidIn(long delay)
    {
        return _reply.map(r -> r.isValidIn(delay)).orElse(Boolean.TRUE);
    }

    public PnfsId getPnfsId()
    {
        return _request.getFileAttributes().getPnfsId();
    }

    public FileAttributes getFileAttributes()
    {
        return _request.getFileAttributes();
    }

    public void setFileAttributes(FileAttributes attributes)
    {
        _request.setFileAttributes(attributes);
    }

    public ProtocolInfo getProtocolInfo()
    {
        return _request.getProtocolInfo();
    }

    public Subject getSubject()
    {
        return _request.getSubject();
    }

    public String getRequestId()
    {
        return _request.getRequestId();
    }

    public long getLifetime()
    {
        return _request.getLifetime();
    }

    public long getPinId()
    {
        return _pin.getPinId();
    }

    public String getPool()
    {
        return _pin.getPool();
    }

    public String getSticky()
    {
        return _pin.getSticky();
    }

    public boolean isStagingDenied()
    {
        return _request.isStagingDenied();
    }

    public PoolMgrSelectReadPoolMsg.Context getReadPoolSelectionContext()
    {
        return _readPoolSelectionContext;
    }

    public void setReadPoolSelectionContext(PoolMgrSelectReadPoolMsg.Context context)
    {
        _readPoolSelectionContext = context;
    }

    public Date freezeExpirationTime()
    {
        long now = System.currentTimeMillis();
        long lifetime = getLifetime();
        Date date = (lifetime == -1) ? null : new Date(now + lifetime);
        _request.setExpirationTime(date);
        return date;
    }

    public Date getExpirationTime()
    {
        return _request.getExpirationTime();
    }

    public void fail(int rc, String error)
    {
        _reply.ifPresent(r -> r.fail(_request, rc, error));
        _log.warn("Failed to pin {}: {} [{}]", _pin.getPnfsId(), error, rc);
    }

    public void success()
    {
        _reply.ifPresent(r -> {
                    _request.setPin(_pin);
                    r.reply(_request);
                });
        _log.info("Pinned {} on {} ({})", _pin.getPnfsId(), _pin.getPool(), _pin.getPinId());
    }
}
