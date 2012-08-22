package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorResponse extends AbstractResponseMessage
{
    private final static Logger _log = LoggerFactory.getLogger(ErrorResponse.class);

    private final int _errnum;
    private final String _errmsg;

    public ErrorResponse(int sId, int errnum, String errmsg)
    {
        super(sId, XrootdProtocol.kXR_error, errmsg.length() + 4);

        _errnum = errnum;
        _errmsg = errmsg;

        putSignedInt(errnum);
        putCharSequence(errmsg);

        _log.info("Xrootd-Error-Response: ErrorNr="+ errnum +" ErrorMsg="+ errmsg);
    }

    @Override
    public String toString()
    {
        return String.format("error[%d,%s]", _errnum, _errmsg);
    }
}
