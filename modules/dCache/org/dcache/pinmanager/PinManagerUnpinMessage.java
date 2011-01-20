package org.dcache.pinmanager;

import java.util.EnumSet;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import static org.dcache.namespace.FileAttribute.*;

public class PinManagerUnpinMessage extends Message
{
    static final long serialVersionUID = 5504172816234212007L;

    private Long _pinId;
    private String _requestId;
    private FileAttributes _fileAttributes;

    public PinManagerUnpinMessage(FileAttributes fileAttributes)
    {
        _fileAttributes = fileAttributes;
    }

    public void setPinId(long pinId)
    {
        _pinId = pinId;
    }

    public Long getPinId()
    {
        return _pinId;
    }

    public void setRequestId(String requestId)
    {
        _requestId = requestId;
    }

    public String getRequestId()
    {
        return _requestId;
    }

    public PnfsId getPnfsId()
    {
        return _fileAttributes.getPnfsId();
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    @Override
    public String toString()
    {
        return "PinManagerUnpinMessage[" + _requestId + "," + _pinId + "," + _fileAttributes + "]";
    }

    public static EnumSet<FileAttribute> getRequiredAttributes()
    {
        return EnumSet.of(PNFSID);
    }
}
