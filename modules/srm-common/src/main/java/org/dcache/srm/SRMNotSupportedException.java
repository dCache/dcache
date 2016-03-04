package org.dcache.srm;

import org.dcache.srm.v2_2.TStatusCode;

public class SRMNotSupportedException extends SRMException
{
    private static final long serialVersionUID = -1902276688274095648L;

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

    @Override
    public TStatusCode getStatusCode()
    {
        return TStatusCode.SRM_NOT_SUPPORTED;
    }
}
