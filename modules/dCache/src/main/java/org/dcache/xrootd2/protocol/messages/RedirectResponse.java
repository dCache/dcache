package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedirectResponse extends AbstractResponseMessage
{
    private final static Logger _logger =
        LoggerFactory.getLogger(RedirectResponse.class);

    public RedirectResponse(int sId, String host, int port)
    {
        this(sId, host, port, "", "");
    }

    public RedirectResponse(int sId, String host, int port, String opaque, String token)
    {
        super(sId, XrootdProtocol.kXR_redirect,
              4 + host.length() + opaque.length() + token.length() + 2);

        putSignedInt(port);
        _logger.info("Sending the following host information to the client: {}", host);
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