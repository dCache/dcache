package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;

public class RedirectResponse extends AbstractResponseMessage
{
    public RedirectResponse(int sId, String host, int port)
    {
        this(sId, host, port, "", "");
    }

    public RedirectResponse(int sId, String host, int port, String opaque, String token)
    {
        super(sId, XrootdProtocol.kXR_redirect,
              4 + host.length() + opaque.length() + token.length() + 2);

        putSignedInt(port);
        putCharSequence(host);

        if (!opaque.equals("")) {
            putCharSequence("?");
            putCharSequence(opaque);
        }

        if (!token.equals("")) {
            if (opaque.equals("")) {
                putCharSequence("?");
            }

            putCharSequence("?");
            putCharSequence(token);
        }
    }
}