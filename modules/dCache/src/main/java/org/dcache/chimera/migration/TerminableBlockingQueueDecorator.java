package org.dcache.chimera.migration;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class that implements TerminableBlockingQueue interface by
 * wrapping an existing BlockingQueue object.
 */
public class TerminableBlockingQueueDecorator<E> extends
        AbstractBlockingQueueDecorator<E> implements TerminableBlockingQueue<E> {

    private static Logger _log = LoggerFactory.getLogger( TerminableBlockingQueueDecorator.class);

    private static final int DEFAULT_CONSUMERS_COUNT = 1;

    private final E _sentinel;
    private final int _consumers;

    private boolean _isTerminated;

    public TerminableBlockingQueueDecorator( BlockingQueue<E> queue, E sentinal) {
        this( queue, sentinal, DEFAULT_CONSUMERS_COUNT);
    }

    public TerminableBlockingQueueDecorator( BlockingQueue<E> queue,
                                             E sentinel, int consumers) {
        super( queue);
        _sentinel = sentinel;
        _consumers = consumers;
    }

    @Override
    public boolean hasTerminateWith( E item) {
        return (_isTerminated || item == _sentinel);
    }

    @Override
    public synchronized boolean isTerminated() {
        return _isTerminated;
    }

    @Override
    public synchronized void terminate() {
        _isTerminated = true;

        populateWithSentinels();
    }

    /**
     * Populate the queue with the required number of sentinels. If there is
     * insufficient space then existing non-sentinel objects are taken from
     * the queue to provide sufficient space.
     */
    private void populateWithSentinels() {
        try {
            for( int i = 0; i < _consumers; i++)
                addSentinel();
        } catch (InterruptedException e) {
            System.out.println( "Interrupted while enqueing sentinel items");
        }
    }


    /**
     * Try adding a sentinel item to the queue.  If that isn't possible due to queue
     * limit constraints, attempt to remove non-sentinel items from the queue to
     * make sufficient space.  If no items can be removed from the queue (e.g.,
     * the next item on the queue is a sentinel) then insert the sentinel in
     * a blocking fashion.
     */
    private void addSentinel() throws InterruptedException {
        boolean haveAddedSentinel = false;

        while( !haveAddedSentinel) {
            haveAddedSentinel = _queue.offer( _sentinel);

            int removedCount=0;
            if( !haveAddedSentinel) {
                while (peek() != null && peek() != _sentinel) {
                    take();
                    removedCount++;
                }

                if( removedCount == 0) {
                    _log.info( "Blocking on TerminableBlockingQueue accepting sentinel");
                    _queue.put( _sentinel);
                    haveAddedSentinel = true;
                }
            }
        }
    }
}
