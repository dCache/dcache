/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * <p>A list whose elements, once assigned, cannot be re-indexed.</p>
 *
 * <p>Add operations append to the end of the list as usual, but removes
 *      do not collapse the list.  Hence index numbers are monotonically
 *      increasing at each add.</p>
 *
 * <p>Iteration is based on a realized list detached from the underlying
 *      data structures, and thus side-effects through the iterator will
 *      not change the list itself.</p>
 *
 * <p>This realized (iterable) list is guaranteed to respect the order
 *      of insertion, but the value returned by indexOf() may not be
 *      equal to the implicit index of the iterable list returned for
 *      iteration or streaming unless the value of <code>includeNulls</code>
 *      is set to true (false by default).  In that case, it then becomes
 *      imperative that the caller check for <code>null</code> values, as
 *      any index assigned to an element which has been removed will be
 *      marked by <code>null</code>.</p>
 *
 * <p>Any operation which requires the reassignment of list positions
 *      to existing elements is unsupported.  Mutation of the list via the
 *      <code>set</code> operation is also unsupported.</p>
 *
 * <p>This list contains unique elements.  Adding the same element twice
 *      overwrites the previous index. Nulls cannot be added to the list.</p>
 *
 * <p>Not thread-safe.</p>
 */
public final class NonReindexableList<E> implements List<E> {
    private static final String UNSUPPORTED_ERROR_MSG
                    = "This list can only be modified by appending or removing.";

    private final Map<E, Integer> index = new HashMap<>();
    private final Map<Integer, E> list = new HashMap<>();

    private int counter = 0;
    private boolean includeNulls = false;

    @Override
    public boolean add(E e) {
        return append(e);
    }

    @Override
    public void add(int index, E element) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        int added = appendAll(c);
        return added > 0;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
    }

    @Override
    public boolean contains(Object o) {
        return index.containsKey(o);
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        for (Object o : collection) {
            if (!index.containsKey(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        return list.values().equals(o);
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
    }

    @Override
    public E get(int index) {
        if (index >= counter) {
            throw new IndexOutOfBoundsException(index + " >= " + counter);
        }
        E element = list.get(index);
        if (element == null && !includeNulls) {
            throw new NoSuchElementException("at index " + index);
        }
        return element;
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    @Override
    public int indexOf(Object o) {
        if (index.containsKey(o)) {
            return index.get(o);
        }
        /*
         *  Differs from the normal list, which would return -1.
         */
        throw new NoSuchElementException(String.valueOf(o));
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    public boolean isIncludeNulls() {
        return includeNulls;
    }

    @Override
    public Iterator<E> iterator() {
        return realizeList().iterator();
    }

    @Override
    public int lastIndexOf(Object o) {
        return indexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return realizeList().listIterator();
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return realizeList().listIterator(index);
    }

    @Override
    public Stream<E> parallelStream() {
        return realizeList().parallelStream();
    }

    @Override
    public boolean remove(Object element) {
        Integer i = index.remove(element);
        if (i != null) {
            list.remove(i);
            return true;
        }
        return false;
    }

    @Override
    public E remove(int index) {
        E element = list.remove(index);
        if (element != null) {
            this.index.remove(element);
        }
        return element;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        int changes = 0;
        for (Object o : c) {
            if (remove(o)) {
                ++changes;
            }
        }
        return changes > 0;
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E set(int index, E element) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MSG);
    }

    public void setIncludeNulls(boolean includeNulls) {
        this.includeNulls = includeNulls;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public void sort(Comparator<? super E> c) {
        throw new UnsupportedOperationException(
                        "This list uses a fixed monotonically increasing "
                                        + "order of insertion indexing.");
    }

    @Override
    public Spliterator<E> spliterator() {
        return realizeList().spliterator();
    }

    @Override
    public Stream<E> stream() {
        return realizeList().stream();
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return realizeList().subList(fromIndex, toIndex);
    }

    @Override
    public Object[] toArray() {
        return list.values().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return list.values().toArray(a);
    }

    private boolean append(E element) {
        if (element == null) {
            throw new IllegalArgumentException("Cannot add null "
                            + "values to this list.");
        }

        if (index.containsKey(element)) {
            return false;
        }

        int next = counter++;
        list.put(next, element);
        index.put(element, next);
        return true;
    }

    private int appendAll(Collection<? extends E> collection) {
        int added = 0;
        for (E element: collection) {
            if (append(element)) {
                ++added;
            }
        }

        return added;
    }

    private List<E> realizeList() {
        List<E> iterable = new ArrayList<>();
        for (int i = 0; i < counter; i++) {
            E element = list.get(i);
            if (element != null || includeNulls) {
                iterable.add(element);
            }
        }
        return iterable;
    }
}
