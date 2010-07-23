package org.dcache.util;

import java.util.AbstractList;

/**
 * Wraps a random access list to make it immutable.
 *
 * Similar to Collections.unmodifiableList, but provides an explicit
 * type for immutable lists.
 */
public class ImmutableList<T>
    extends AbstractList<T>
{
    private final AbstractList<T> _list;

    /**
     * Wraps a random access list. The type of the wrapped list is
     * AbstractList as this guarantees that the list is a random
     * access list.
     *
     * @param list the random access list to wrap
     */
    public ImmutableList(AbstractList<T> list)
    {
        _list = list;
    }

    public T get(int index)
    {
        return _list.get(index);
    }

    public int size()
    {
        return _list.size();
    }
}