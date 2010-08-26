package org.dcache.xrootd.protocol.messages;

import org.dcache.xrootd.protocol.XrootdProtocol;

public class DummyRequest extends AbstractRequestMessage {

    public DummyRequest() {
        super(new int[XrootdProtocol.CLIENT_REQUEST_LEN], null);
    }

}
