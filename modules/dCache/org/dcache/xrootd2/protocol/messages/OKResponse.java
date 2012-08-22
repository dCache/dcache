package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;

public class OKResponse extends AbstractResponseMessage
{
    public OKResponse(int sId)
    {
        super(sId, XrootdProtocol.kXR_ok, 0);
    }

    @Override
    public String toString()
    {
        return "ok";
    }
}
