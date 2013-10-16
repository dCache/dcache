package org.dcache.srm;

public class SRMNonEmptyDirectoryException extends SRMException
{
    public SRMNonEmptyDirectoryException()
    {
    }

    public SRMNonEmptyDirectoryException(String message)
    {
        super(message);
    }

    public SRMNonEmptyDirectoryException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SRMNonEmptyDirectoryException(Throwable cause)
    {
        super(cause);
    }
}
