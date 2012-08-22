package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;

public class ProtocolResponse extends AbstractResponseMessage
{
    private final int _flags;

    public ProtocolResponse(int sId, int flags)
    {
        super(sId, XrootdProtocol.kXR_ok, 8);
        _flags = flags;
        putSignedInt(XrootdProtocol.PROTOCOL_VERSION);
        putSignedInt(flags);
    }

    @Override
    public String toString()
    {
        return String.format("protocol-response[%d]", _flags);
    }
}
