package org.dcache.util.expression;

public class Symbol
{
    private final String _name;
    private final Type _type;
    private final Object _value;

    public Symbol(String name, Type type, Object value)
    {
        _name = name;
        _type = type;
        _value = value;
    }

    public String getName()
    {
        return _name;
    }

    public Type getType()
    {
        return _type;
    }

    public Object getValue()
    {
        return _value;
    }
}
