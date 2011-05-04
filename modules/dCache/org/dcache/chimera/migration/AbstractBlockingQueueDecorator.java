package org.dcache.chimera.migration;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A framework class that may be extended to build an arbitrary BlockingQueue decorator.
 */
public class AbstractBlockingQueueDecorator<E> implements BlockingQueue<E> {

    protected BlockingQueue<E> _queue;

    protected AbstractBlockingQueueDecorator( BlockingQueue<E> queue) {
        _queue = queue;
    }

    @Override
    public boolean add( E item) {
        return _queue.add( item);
    }

    @Override
    public boolean contains( Object item) {
        return _queue.contains( item);
    }

    @Override
    public int drainTo( Collection<? super E> c) {
        return _queue.drainTo( c);
    }

    @Override
    public int drainTo( Collection<? super E> c, int maxElements) {
        return _queue.drainTo( c, maxElements);
    }

    @Override
    public boolean offer( E item) {
        return _queue.offer( item);
    }

    @Override
    public boolean offer( E item, long timeout, TimeUnit unit)
            throws InterruptedException {
        return _queue.offer( item, timeout, unit);
    }

    @Override
    public E poll( long timeout, TimeUnit unit) throws InterruptedException {
        return _queue.poll( timeout, unit);
    }

    @Override
    public void put( E item) throws InterruptedException {
        _queue.put( item);
    }

    @Override
    public int remainingCapacity() {
        return _queue.remainingCapacity();
    }

    @Override
    public boolean remove(Object item) {
        return _queue.remove(item);
    }

    @Override
    public E take() throws InterruptedException {
        return _queue.take();
    }

    @Override
    public E element() {
        return _queue.element();
    }

    @Override
    public E peek() {
        return _queue.peek();
    }

    @Override
    public E poll() {
        return _queue.poll();
    }

    @Override
    public E remove() {
        return _queue.remove();
    }

    @Override
    public boolean addAll( Collection<? extends E> c) {
        return _queue.addAll( c);
    }

    @Override
    public void clear() {
        _queue.clear();
    }

    @Override
    public boolean containsAll( Collection<?> c) {
        return _queue.containsAll( c);
    }

    @Override
    public boolean isEmpty() {
        return _queue.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return _queue.iterator();
    }

    @Override
    public boolean removeAll( Collection<?> c) {
        return _queue.removeAll( c);
    }

    @Override
    public boolean retainAll( Collection<?> c) {
        return _queue.retainAll( c);
    }

    @Override
    public int size() {
        return _queue.size();
    }

    @Override
    public Object[] toArray() {
        return _queue.toArray();
    }

    @Override
    public <T> T[] toArray( T[] a) {
        return _queue.toArray( a);
    }
}
