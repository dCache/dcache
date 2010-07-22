package org.dcache.pool.migration;

import java.util.Set;
import java.util.HashSet;

import org.apache.commons.jexl2.MapContext;

public class MapContextWithConstants extends MapContext
{
    private Set<String> _contants = new HashSet<String>();

    public void addConstant(String name, Object value)
    {
        if (get(name) != null) {
            throw new IllegalStateException("Already defined as variable: " + name);
        }
        _contants.add(name);
        super.set(name, value);
    }

    public void set(String name, Object value)
    {
        if (_contants.contains(name)) {
            throw new IllegalStateException("Cannot modify constant: " + name);
        }

        super.set(name, value);
    }
}