package org.dcache.srm;

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
}
