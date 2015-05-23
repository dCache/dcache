package org.dcache.commons.util;

import com.google.common.annotations.VisibleForTesting;

import java.util.Date;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Atomic counter that supports waiting for the counter to change.
 */
public class AtomicCounter
{
    private final Lock _lock = new ReentrantLock();
    private final Condition _updated = _lock.newCondition();
    private int _counter;

    /**
     * Increment the counter by one.
     */
    public void increment()
    {
        _lock.lock();
        try {
            inLock();
            _counter++;
            _updated.signalAll();
        } finally {
            _lock.unlock();
        }
    }

    /**
     * Increment the current value of the counter.
     */
    public int get()
    {
        _lock.lock();
        try {
            inLock();
            return _counter;
        } finally {
            _lock.unlock();
        }
    }

    /**
     * Waits for the counter to change to a value different from
     * <code>value</code>.
     *
     * The method returns when one of the following happens:
     *
     * * The current counter value is different from the
     *   <code>value</code> argument; or
     *
     * * Some other thread invokes the <code>increment</code> method for
     *   this AtomicCounter; or
     *
     * * Some other thread interrupts the current thread; or
     *
     * * The specified deadline elapses; or
     *
     * * A "spurious wakeup" occurs.
     *
     * @param value the value to wait for the counter to change away from
     * @param deadline the absolute time to wait until
     * @return true if the counter has a different value than {@code value} upon return
     * @throw InterruptedException if the current thread is interrupted
     */
    public boolean awaitChangeUntil(int value, Date deadline)
        throws InterruptedException
    {
        _lock.lock();
        try {
            inLock();
            return _counter != value || _updated.awaitUntil(deadline);
        } finally {
            _lock.unlock();
        }
    }

    @VisibleForTesting
    void inLock()
    {
    }
}
