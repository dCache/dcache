package org.dcache.xrootd2.protocol.messages;

public class AuthentiticationResponse extends AbstractResponseMessage
{
    public AuthentiticationResponse(int sId, int stat, int length)
    {
        super(sId, stat, length);
    }
}
