package org.dcache.gplazma;

/**
 *  This class provides a base exception for any internal gPlazma exception
 *  that is not intended to propagate outside of gPlazma
 */
public class GPlazmaInternalException extends Exception
{
    private static final long serialVersionUID = 1L;

    public GPlazmaInternalException(String message)
    {
        super(message);
    }


    public GPlazmaInternalException(String message, Throwable cause)
    {
        super(message, cause);
    }


    public GPlazmaInternalException(Throwable cause)
    {
        super(cause);
    }
}
