package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class RedirectResponse extends AbstractResponseMessage {

    public RedirectResponse(int sId, String host, int port) {
        this(sId, host, port, null, null);
    }

    public RedirectResponse(int sId, String host, int port, String opaque, String token) {
        super(sId, XrootdProtocol.kXR_redirect, 4 + host.length());

        putSignedInt(port);
        putCharSequence(host);

        StringBuffer sb = new StringBuffer();

        if (opaque != null && !opaque.equals("")) {

            sb.append("?");
            sb.append(opaque);
        }

        if (token != null && !token.equals("")) {

            if (opaque == null || opaque.equals("")) {
                sb.append("?");
            }

            sb.append("?");
            sb.append(token);
        }


        if (sb.length() > 0) {

            int newDataLength = host.length() + 4 + sb.length();

            //			increase size of data portion
            resizeData(newDataLength);

            //			add opaque and/or token data
            putCharSequence(sb.toString());

            //			set new data size in header
            writeToData(false);
            resetCurrent(4);
            putSignedInt(newDataLength);
        }

    }

}