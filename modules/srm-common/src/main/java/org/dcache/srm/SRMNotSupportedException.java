package org.dcache.srm;

public class SRMNotSupportedException extends SRMException
{
    public SRMNotSupportedException()
    {
    }

    public SRMNotSupportedException(String message)
    {
        super(message);
    }

    public SRMNotSupportedException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SRMNotSupportedException(Throwable cause)
    {
        super(cause);
    }
}
