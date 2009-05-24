package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class ReadResponse extends AbstractResponseMessage {

    public ReadResponse(int sId, int status, byte[] data, int dataLength) {
        super(sId, status);

        if (status != XrootdProtocol.kXR_ok && status != XrootdProtocol.kXR_oksofar)
            throw new IllegalArgumentException("doesn't seem to be a kXR_ok or kXR_oksofar response message");

        setData(data, dataLength);

    }

}
