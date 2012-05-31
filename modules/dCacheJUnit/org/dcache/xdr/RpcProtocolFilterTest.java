package org.dcache.xdr;

import com.sun.grizzly.Context;
import com.sun.grizzly.Controller.Protocol;
import com.sun.grizzly.NIOContext;
import com.sun.grizzly.ProtocolFilter;
import com.sun.grizzly.ProtocolParser;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class RpcProtocolFilterTest {

    private ProtocolFilter _filter;
    private Context _ctx;

    @Before
    public void setUp() {
        _filter = new RpcProtocolFilter();
        _ctx = new MockedContext();
    }

    @Test
    public void shouldStopProcessingOnBadXdr() throws IOException {
        _ctx.setAttribute(ProtocolParser.MESSAGE, createBadXdr());
        assertFalse(_filter.execute(_ctx));
    }

    private static class MockedContext extends NIOContext {

        @Override
        public Protocol getProtocol() {
            return Protocol.CUSTOM;
        }
    }

    private Xdr createBadXdr() {
        Xdr xdr = new XdrBuffer(32);
        xdr.beginEncoding();
        RpcMessage message = new RpcMessage(0, 2); // 2 is an invalid value
        message.xdrEncode(xdr);
        xdr.endEncoding();
        return xdr;
    }
}
