package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;

import org.apache.log4j.Logger;

public class ErrorResponse extends AbstractResponseMessage
{
    private final static Logger _log = Logger.getLogger(ErrorResponse.class);

    public ErrorResponse(int sId, int errnum, String errmsg)
    {
        super(sId, XrootdProtocol.kXR_error, errmsg.length() + 4);

        putSignedInt(errnum);

        putCharSequence(errmsg);

        _log.info("Xrootd-Error-Response: ErrorNr="+errnum+" ErrorMsg="+errmsg);
    }
}
