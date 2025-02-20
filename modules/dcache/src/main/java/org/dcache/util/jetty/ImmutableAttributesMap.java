package org.dcache.util.jetty;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import org.eclipse.jetty.util.Attributes;

/**
 * Helper class for Spring injecting Jetty Attributes.
 */
public class ImmutableAttributesMap implements Attributes {

    private final Map<String, Object> map;

    public ImmutableAttributesMap(Map<String, Object> map) {
        this.map = Map.copyOf(map);
    }

    @Override
    public void removeAttribute(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String name, Object attribute) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(String name) {
        return map.get(name);
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        return Collections.enumeration(getAttributeNameSet());
    }

    public Set<String> getAttributeNameSet() {
        return keySet();
    }

    @Override
    public void clearAttributes() {
        throw new UnsupportedOperationException();
    }

    public int size() {
        return map.size();
    }

    @Override
    public String toString() {
        return map.toString();
    }

    private Set<String> keySet() {
        return map.keySet();
    }
}
