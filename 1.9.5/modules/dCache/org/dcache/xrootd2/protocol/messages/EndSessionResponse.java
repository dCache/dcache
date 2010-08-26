package org.dcache.xrootd2.protocol.messages;

public class EndSessionResponse extends AbstractResponseMessage
{
    public EndSessionResponse(int sId, int stat, int length)
    {
        super(sId, stat, length);
    }
}
