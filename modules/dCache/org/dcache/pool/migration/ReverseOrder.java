package org.dcache.pool.migration;

import java.util.Comparator;
import org.dcache.pool.repository.CacheEntry;

class ReverseOrder<T> implements Comparator<T>
{
    private Comparator<T> _inner;

    public ReverseOrder(Comparator<T> inner)
    {
        _inner = inner;
    }

    public int compare(T e1, T e2)
    {
        return _inner.compare(e2, e1);
    }
}