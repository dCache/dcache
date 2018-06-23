/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A Set that provides a view of some underlying Set that protects against
 * deleting elements from that set.
 */
public class AppendOnlySet<T> implements Set<T>
{
    private final Set<T> inner;

    public AppendOnlySet(Set<T> inner)
    {
        this.inner = inner;
    }

    @Override
    public int size() {
        return inner.size();
    }

    @Override
    public boolean isEmpty() {
        return inner.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return inner.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<T> i = inner.iterator();
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public T next() {
                return i.next();
            }
        };
    }

    @Override
    public boolean add(T e) {
        return inner.add(e);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("May not remove elements");
    }

    @Override
    public boolean containsAll(Collection c) {
        return inner.containsAll(c);
    }

    @Override
    public boolean addAll(Collection c) {
        return inner.addAll(c);
    }

    @Override
    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException("May not remove elements");
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException("May not remove elements");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("May not remove elements");
    }

    @Override
    public Object[] toArray() {
        return inner.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return inner.toArray(a);
    }

    @Override
    public Spliterator<T> spliterator() {
        return inner.spliterator();
    }

    @Override
    public Stream<T> parallelStream() {
        return inner.parallelStream();
    }

    @Override
    public Stream<T> stream() {
        return inner.stream();
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException("May not remove elements");
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        inner.forEach(action);
    }
}
