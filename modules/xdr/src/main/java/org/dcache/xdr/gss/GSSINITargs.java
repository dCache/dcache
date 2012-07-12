package org.dcache.xdr.gss;

import java.io.IOException;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.XdrAble;
import org.dcache.xdr.XdrDecodingStream;
import org.dcache.xdr.XdrEncodingStream;

/**
 * The data for a GSS context creation request.
 */
public class GSSINITargs implements XdrAble {

    private byte[] _token;

    public byte[] getToken() {
        return _token;
    }

    public void setToken(byte[] token) {
        this._token = token;
    }

    public GSSINITargs() {
    }

    public GSSINITargs(byte[] token) {
        this._token = token;
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _token = xdr.xdrDecodeDynamicOpaque();
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeDynamicOpaque(_token);
    }

}
