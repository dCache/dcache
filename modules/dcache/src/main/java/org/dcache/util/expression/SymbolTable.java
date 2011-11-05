package org.dcache.util.expression;

import java.util.HashMap;

public class SymbolTable extends HashMap<String,Symbol>
{
    public void put(String name, long value)
    {
        put(name, (double) value);
    }

    public void put(String name, boolean value)
    {
        put(name, new Symbol(name, Type.BOOLEAN, value));
    }

    public void put(String name, double value)
    {
        put(name, new Symbol(name, Type.DOUBLE, value));
    }

    public void put(String name, String value)
    {
        put(name, new Symbol(name, Type.STRING, value));
    }
}