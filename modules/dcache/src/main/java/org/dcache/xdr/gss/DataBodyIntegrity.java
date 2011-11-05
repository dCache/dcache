package org.dcache.xdr.gss;

import java.io.IOException;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.XdrAble;
import org.dcache.xdr.XdrDecodingStream;
import org.dcache.xdr.XdrEncodingStream;

/**
 * RPCGSS_SEC data body for integrity QOS as defined in RFC 2203
 */
public class DataBodyIntegrity implements XdrAble {

    private byte[] _data;
    private byte[] _checksum;

    public DataBodyIntegrity() {
    }

    public DataBodyIntegrity(byte[] data, byte[] checksum) {
        this._data = data;
        this._checksum = checksum;
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _data = xdr.xdrDecodeDynamicOpaque();
        _checksum = xdr.xdrDecodeDynamicOpaque();
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeDynamicOpaque(_data);
        xdr.xdrEncodeDynamicOpaque(_checksum);
    }

    public byte[] getChecksum() {
        return _checksum;
    }

    public byte[] getData() {
        return _data;
    }
}
