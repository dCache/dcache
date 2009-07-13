package org.dcache.xrootd2.protocol.messages;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import java.nio.channels.ScatteringByteChannel;
import java.io.IOException;

public class ReadResponse extends AbstractResponseMessage
{
    public ReadResponse(int sId, int length)
    {
        super(sId, kXR_ok, length);
    }

    /**
     * Set the status field to indicate whether the response is
     * complete or not.
     */
    public void setIncomplete(boolean incomplete)
    {
        setStatus(incomplete ? kXR_oksofar : kXR_ok);
    }

    /**
     * Reads bytes from a channel into the response buffer.
     */
    public int writeBytes(ScatteringByteChannel in, int length)
        throws IOException
    {
        return _buffer.writeBytes(in, length);
    }
}
