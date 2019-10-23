package org.dcache.pinmanager;

import java.util.Date;
import java.util.EnumSet;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.model.Pin;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.namespace.FileAttribute.PNFSID;

public class PinManagerPinMessage extends Message
{
    private static final long serialVersionUID = -146552359952271936L;

    private FileAttributes _fileAttributes;
    private final ProtocolInfo _protocolInfo;
    private long _lifetime;
    private long _pinId;
    private String _pool;
    private final String _requestId;
    private Date _expirationTime;
    private boolean _replyWhenStarted;
    private boolean _denyStaging;

    public PinManagerPinMessage(FileAttributes fileAttributes,
                                ProtocolInfo protocolInfo,
                                String requestId,
                                long lifetime)
    {
        _fileAttributes = checkNotNull(fileAttributes);
        _protocolInfo = checkNotNull(protocolInfo);
        _requestId = requestId;
        _lifetime = lifetime;
    }

    /**
     * Choose whether to wait for the pin to be established before returning.
     * If value is true then do not wait for file to be staged, but return
     * straight away.  Calling getPool, getPinId and getExpirationTime will all
     * return null, unless this request is a retry and the original request
     * succeeded (have establishing a pin) in which case the details of that
     * pin are available through those methods.
     * <p>
     * If set to false (the default) then the message returns once the pin
     * request has been processed and the pin established, or if there was an
     * some error.
     * @param value whether to reply after the pinning task has been started.
     */
    public void setReplyWhenStarted(boolean value)
    {
        _replyWhenStarted = value;
    }

    public boolean isReplyWhenStarted()
    {
        return _replyWhenStarted;
    }

    public void setDenyStaging(boolean value)
    {
        _denyStaging = value;
    }

    public boolean isStagingDenied()
    {
        return _denyStaging;
    }

    public String getRequestId()
    {
        return _requestId;
    }

    public void setLifetime(long lifetime)
    {
        _lifetime = lifetime;
    }

    public long getLifetime()
    {
        return _lifetime;
    }

    public PnfsId getPnfsId()
    {
        return _fileAttributes.getPnfsId();
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public void setFileAttributes(FileAttributes attributes)
    {
        _fileAttributes = checkNotNull(attributes);
    }

    public ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    public String getPool()
    {
        return _pool;
    }

    public void setPool(String pool)
    {
        _pool = pool;
    }

    public long getPinId()
    {
        return _pinId;
    }

    public void setPinId(long pinId)
    {
        _pinId = pinId;
    }

    public void setExpirationTime(Date expirationTime)
    {
        _expirationTime = expirationTime;
    }

    public Date getExpirationTime()
    {
        return _expirationTime;
    }

    public void setPin(Pin pin)
    {
        setPool(pin.getPool());
        setPinId(pin.getPinId());
        setExpirationTime(pin.getExpirationTime());
    }

    @Override
    public String toString()
    {
        return "PinManagerPinMessage["+_fileAttributes + "," +
            _protocolInfo + "," + _lifetime + "]";
    }

    public static EnumSet<FileAttribute> getRequiredAttributes()
    {
        EnumSet<FileAttribute> attributes = EnumSet.of(PNFSID);
        attributes.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
        return attributes;
    }
}
