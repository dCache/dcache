package org.dcache.util.expression;

public class UnknownIdentifierException extends Exception
{
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