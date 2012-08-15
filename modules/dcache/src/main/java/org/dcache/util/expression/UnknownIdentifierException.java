package org.dcache.util.expression;

public class UnknownIdentifierException extends Exception
{
    private static final long serialVersionUID = -6503622624033170074L;
    private final String _identifier;

    public UnknownIdentifierException(String s) {
        super("Unknown identifier: " + s);
        _identifier = s;
    }

    public String getIdentifier()
    {
        return _identifier;
    }
}