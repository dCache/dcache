package org.dcache.xdr.gss;

import java.io.IOException;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.XdrAble;
import org.dcache.xdr.XdrDecodingStream;
import org.dcache.xdr.XdrEncodingStream;

/**
 * RPCGSS_SEC data body for privacy QOS as defined in RFC 2203
 */
public class DataBodyPrivacy implements XdrAble {

    byte[] _data;

    public DataBodyPrivacy() {
    }

    public DataBodyPrivacy(byte[] data) {
        this._data = data;
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _data = xdr.xdrDecodeDynamicOpaque();
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {

        xdr.xdrEncodeDynamicOpaque(_data);
    }

    public byte[] getData() {
        return _data;
    }
}
