package org.dcache.gplazma.oidc.exceptions;

public class OidcException extends Exception {
    private static final long serialVersionUID = 1L;

    public OidcException(String message)
    {
        super(message);
    }

    public OidcException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public OidcException(Throwable cause)
    {
        super(cause);
    }

    public OidcException(String host, String message)
    {
        super("(\"" + host + "\", " + message + ")");
    }
}
