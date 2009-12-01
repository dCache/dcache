package org.dcache.chimera.migration;

/**
 * A framework class that may be extended to build an abstract decorator of
 * some TerminableBlockingQueue.
 */
public class AbstractTerminableBlockingQueueDecorator<E> extends
        AbstractBlockingQueueDecorator<E> implements TerminableBlockingQueue<E> {

    protected TerminableBlockingQueue<E> _terminableQueue;

    protected AbstractTerminableBlockingQueueDecorator( TerminableBlockingQueue<E> queue) {
        super( queue);
        _terminableQueue = queue;
    }

    @Override
    public boolean hasTerminateWith( E item) {
        return _terminableQueue.hasTerminateWith( item);
    }

    @Override
    public boolean isTerminated() {
        return _terminableQueue.isTerminated();
    }

    @Override
    public void terminate() {
        _terminableQueue.terminate();
    }
}
