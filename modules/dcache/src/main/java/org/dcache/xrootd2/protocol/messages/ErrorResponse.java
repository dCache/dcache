package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorResponse extends AbstractResponseMessage
{
    private final static Logger _log = LoggerFactory.getLogger(ErrorResponse.class);

    public ErrorResponse(int sId, int errnum, String errmsg)
    {
        super(sId, XrootdProtocol.kXR_error, errmsg.length() + 4);

        putSignedInt(errnum);

        putCharSequence(errmsg);

        _log.info("Xrootd-Error-Response: ErrorNr="+errnum+" ErrorMsg="+errmsg);
    }
}
