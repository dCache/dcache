package org.dcache.xdr.gss;

import java.io.IOException;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.XdrAble;
import org.dcache.xdr.XdrDecodingStream;
import org.dcache.xdr.XdrEncodingStream;

/**
 *
 * @author tigran
 */
public class GSSINITres implements  XdrAble {

    private byte[] _handle;
    private int _gssMajor;
    private int _gssMinor;
    private int _sequence;
    private byte[] _token;

    public int getGssMajor() {
        return _gssMajor;
    }

    public void setGssMajor(int gssMajor) {
        this._gssMajor = gssMajor;
    }

    public int getGssMinor() {
        return _gssMinor;
    }

    public void setGssMinor(int gssMinor) {
        this._gssMinor = gssMinor;
    }

    public byte[] getHandle() {
        return _handle;
    }

    public void setHandle(byte[] handle) {
        this._handle = handle;
    }

    public int getSequence() {
        return _sequence;
    }

    public void setSequence(int sequence) {
        this._sequence = sequence;
    }

    public byte[] getToken() {
        return _token;
    }

    public void setToken(byte[] token) {
        this._token = token;
    }

    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _handle = xdr.xdrDecodeDynamicOpaque();
        _gssMajor = xdr.xdrDecodeInt();
        _gssMinor = xdr.xdrDecodeInt();
        _sequence = xdr.xdrDecodeInt();
        _token = xdr.xdrDecodeDynamicOpaque();
    }

    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeDynamicOpaque(_handle);
        xdr.xdrEncodeInt(_gssMajor);
        xdr.xdrEncodeInt(_gssMinor);
        xdr.xdrEncodeInt(_sequence);
        xdr.xdrEncodeDynamicOpaque(_token);
    }

}
