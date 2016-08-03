package org.dcache.util.jetty;

import com.google.common.collect.ImmutableMap;
import org.eclipse.jetty.util.Attributes;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for Spring injecting Jetty Attributes.
 */
public class ImmutableAttributesMap implements Attributes
{
    private final Map<String, Object> map;

    public ImmutableAttributesMap(Map<String, Object> map)
    {
        this.map = ImmutableMap.copyOf(map);
    }

    @Override
    public void removeAttribute(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String name, Object attribute)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(String name)
    {
        return map == null ? null : map.get(name);
    }

    @Override
    public Enumeration<String> getAttributeNames()
    {
        return Collections.enumeration(getAttributeNameSet());
    }

    public Set<String> getAttributeNameSet()
    {
        return keySet();
    }

    @Override
    public void clearAttributes()
    {
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        return map == null ? 0 : map.size();
    }

    @Override
    public String toString()
    {
        return map == null ? "{}" : map.toString();
    }

    private Set<String> keySet()
    {
        return map == null ? Collections.emptySet() : map.keySet();
    }
}
