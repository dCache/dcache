package org.dcache.pool.migration;

import java.util.Comparator;

class ReverseOrder<T> implements Comparator<T>
{
    private final Comparator<T> _inner;

    public ReverseOrder(Comparator<T> inner)
    {
        _inner = inner;
    }

    @Override
    public int compare(T e1, T e2)
    {
        return _inner.compare(e2, e1);
    }
}
