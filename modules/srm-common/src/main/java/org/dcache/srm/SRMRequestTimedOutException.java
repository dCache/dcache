package org.dcache.srm;

import org.dcache.srm.v2_2.TStatusCode;

public class SRMRequestTimedOutException extends SRMException
{
    public SRMRequestTimedOutException()
    {
    }

    public SRMRequestTimedOutException(String message)
    {
        super(message);
    }

    public SRMRequestTimedOutException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SRMRequestTimedOutException(Throwable cause)
    {
        super(cause);
    }

    @Override
    public TStatusCode getStatusCode()
    {
        return TStatusCode.SRM_REQUEST_TIMED_OUT;
    }
}
