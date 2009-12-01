package org.dcache.chimera.migration;

import java.util.concurrent.BlockingQueue;

/**
 * The TerminatableBlockingQueue extends the BlockingQueue interface and
 * introduces the concept of the queue having a finite lifetime. This finite
 * lifetime is that the queue has two states: active and terminated. The
 * queue may transition from active to terminated but, once terminated, it
 * cannot transition to active.
 *
 * A queue may be terminated at any time. Once terminated, one or more
 * special items are sent through the queue to "wake up" threads that are
 * blocking on the queue providing fresh items. On receiving an item from the
 * queue, the thread should check to see if the queue has just terminated.
 * The {@code #hasTerminatedWith} method is provided for this purpose.
 *
 * After a queue has terminated the behaviour of the BlockingQueue methods
 * are not defined. Therefore, the thread(s) injecting items should not
 * attempt to inject any further items after the queue has terminated and any
 * threads receiving items should not attempt to read any further items after
 * receiving an item where hasTerminatedWith(item) returns true.
 */
public interface TerminableBlockingQueue<E> extends BlockingQueue<E> {

    /**
     * Discover whether the queue has terminated.
     *
     * @return true if the queue is terminated, false otherwise.
     */
    public boolean isTerminated();

    /**
     * Instruct the queue to terminate. Subsequent calls to isTerminated()
     * will return true.
     */
    public void terminate();

    /**
     * The queue allows objects to be fetched from the queue. Implementations
     * of this interface may send a special item to "wake up" an client that
     * has undertaken the blocking call
     * {@link java.util.concurrent.BlockingQueue#take}.
     *
     * @return true if the queue has terminated with the given item, false
     *         otherwise.
     */
    public boolean hasTerminateWith( E item);
}
