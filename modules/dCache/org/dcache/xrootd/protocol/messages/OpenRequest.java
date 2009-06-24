package org.dcache.xrootd.protocol.messages;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.Adler32;


import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.util.ParseException;


public class OpenRequest extends AbstractRequestMessage {


    public OpenRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_open)
            throw new IllegalArgumentException("doesn't seem to be a kXR_open message");

    }

    public int getUMask() {
        readFromHeader(true);
        return getUnsignedShort(4);
    }

    public int getOptions() {
        readFromHeader(true);
        return getUnsignedShort(6);
    }

    public String getPath() {

        readFromHeader(false);

        StringBuffer sb = new StringBuffer("");

        for (int i = 0; i < data.length; i++)   {

            //			look for '?' character, indicating beginning of optional opaque information (see xrootd-protocol spec.)
            if (data[i] == 0x3f)
                break;

            sb.append((char) getUnsignedChar(i));
        }

        return sb.toString();
    }

    public Map getOpaque() throws ParseException {

        readFromHeader(false);

        int opaqueStart = -1;

        for (int i = 0; i < getDataLength(); i++) {
            if (getUnsignedChar(i) == '?') {
                opaqueStart = i;
                break;
            }
        }

        Map map = new HashMap();

        if (opaqueStart < 0) {
            return map;
        }

        String opaque = new String(getData(), ++opaqueStart, getDataLength() - opaqueStart);

        int tokenStart;
        int tokenEnd = 0;

        while ((tokenStart = opaque.indexOf('&', tokenEnd)) != -1) {
            tokenEnd = opaque.indexOf('&',++tokenStart);

            if (tokenEnd == -1) {
                tokenEnd = opaque.length();
            }

            int delimiter = opaque.indexOf("=",tokenStart);

            if (delimiter == -1) {
                throw new ParseException("wrong delemiter found. Should be 'key=value'");
            }

            map.put(opaque.substring(tokenStart, delimiter),
                    opaque.substring(delimiter + 1, tokenEnd));


        }

        return map;
    }

    public int hasOpaque() {

        readFromHeader(false);

        for (int i = 0; i < data.length; i++)
            //			look for '?' character, indicating beginning of optional opaque information (see xrootd-protocol spec.)
            if (data[i] == 0x3f)
                return i;

        return -1;
    }

    public boolean isAsync() {
        return (getOptions() & XrootdProtocol.kXR_async) == XrootdProtocol.kXR_async;
    }

    public boolean isCompress() {
        return (getOptions() & XrootdProtocol.kXR_compress) == XrootdProtocol.kXR_compress;
    }

    public boolean isDelete() {
        return (getOptions() & XrootdProtocol.kXR_delete) == XrootdProtocol.kXR_delete;
    }

    public boolean isForce() {
        return (getOptions() & XrootdProtocol.kXR_force) == XrootdProtocol.kXR_force;
    }

    public boolean isNew() {
        return (getOptions() & XrootdProtocol.kXR_new) == XrootdProtocol.kXR_new;
    }

    public boolean isReadOnly() {
        return (getOptions() & XrootdProtocol.kXR_open_read) == XrootdProtocol.kXR_open_read;
    }

    public boolean isReadWrite() {
        return (getOptions() & XrootdProtocol.kXR_open_updt) == XrootdProtocol.kXR_open_updt;
    }

    public boolean isRefresh() {
        return (getOptions() & XrootdProtocol.kXR_refresh) == XrootdProtocol.kXR_refresh;
    }

    /**
     * Calculates the Adler32 checksum over the binary content of the OpenRequest.
     * The checksum covers the following fields: mode, options, plen, path (with opaque included)
     * The StreamID is NOT considered.
     * @return the Adler32-bit checksum
     */
    public long calcChecksum() {
        Adler32 adler32 = new Adler32();

        //		add mode field
        adler32.update(header[4]);
        adler32.update(header[5]);
        //		add options field
        adler32.update(header[6]);
        adler32.update(header[7]);
        //		add path length field
        adler32.update(header[20]);
        adler32.update(header[21]);
        adler32.update(header[22]);
        adler32.update(header[23]);
        //		add data (path + opaque)
        adler32.update(data);

        return adler32.getValue();
    }

    //	public boolean isReadOnly() {
    //
    //		int options = getOptions() & (XrootdProtocol.kXR_new +
    //				  XrootdProtocol.kXR_open_read +
    //				  XrootdProtocol.kXR_open_updt);
    //
    //		return options == XrootdProtocol.kXR_open_read;
    //	}

    //	public boolean isReadWrite() {
    //
    //		int flags = XrootdProtocol.kXR_new + XrootdProtocol.kXR_open_updt;
    //		int options = getOptions() & (flags);
    //
    //		return options == flags;
    //	}


}
