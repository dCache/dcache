package org.dcache.util;

import com.google.common.base.Preconditions;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.*;

/**
 * A simple implementation of an adjustable semaphore.
 *
 * Copyright © 2008, Genius.com
 * @author Marshall Pierce <marshall@genius.com>
 * @see http://blog.teamlazerbeez.com/2009/04/20/javas-semaphore-resizing/
 */
public final class AdjustableSemaphore {

    /**
     * semaphore starts at 0 capacity; must be set by setMaxPermits before use
     */
    private final ResizeableSemaphore semaphore = new ResizeableSemaphore();
    /**
     * how many permits are allowed as governed by this semaphore.
     * Access must be synchronized on this object.
     */
    private int maxPermits;

    /**
     * New instances should be configured with setMaxPermits().
     */
    public AdjustableSemaphore() {
        // no op
    }

    /*
     * Must be synchronized because the underlying int is not thread safe
     */
    /**
     * Set the max number of permits. Must be greater than zero.
     *
     * Note that if there are more than the new max number of permits currently
     * outstanding, any currently blocking threads or any new threads that start
     * to block after the call will wait until enough permits have been released to
     * have the number of outstanding permits fall below the new maximum. In
     * other words, it does what you probably think it should.
     *
     * @param newMax
     */
    public synchronized void setMaxPermits(int newMax) {
        checkArgument(newMax >= 0);

        int delta = newMax - this.maxPermits;

        if (delta == 0) {
            return;
        } else if (delta > 0) {
            // new max is higher, so release that many permits
            this.semaphore.release(delta);
        } else {
            delta *= -1;
            // delta < 0.
            // reducePermits needs a positive #, though.
            this.semaphore.reducePermits(delta);
        }

        this.maxPermits = newMax;
    }

    /**
     * Release a permit back to the semaphore. Make sure not to double-release.
     *
     */
    public void release() {
        this.semaphore.release();
    }

    /**
     * Get a permit, blocking if necessary.
     *
     * @throws InterruptedException
     *             if interrupted while waiting for a permit
     */
    public void acquire() throws InterruptedException {
        this.semaphore.acquire();
    }

    /**
     * @see Semaphore#tryAcquire(int, long, java.util.concurrent.TimeUnit)
     */
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException
    {
        return semaphore.tryAcquire(permits, timeout, unit);
    }

    /**
     * Get a permit this semaphore if one is available at the
     * time of invocation.
     *
     * @returns {@code true} if a permit was acquired and {@code false}
     *         otherwise
     */
    public boolean tryAcquire() {
        return this.semaphore.tryAcquire();
    }

    /**
     * Get a number of allowed permits.
     * @return number of permits
     */
    public synchronized int getMaxPermits() {
        return this.maxPermits;
    }

    /**
     * Get number of permits in use.
     * @return number of permits.
     */
    public synchronized int getUsedPermits() {
        return this.maxPermits - this.semaphore.availablePermits();
    }

    /**
     * A trivial subclass of <code>Semaphore</code> that exposes the reducePermits
     * call to the parent class. Doug Lea says it's ok...
     * http://osdir.com/ml/java.jsr.166-concurrency/2003-10/msg00042.html
     *
     * Copyright © 2009, Genius.com
     *
     * @author Marshall Pierce <marshall@genius.com>
     *
     */
    private static final class ResizeableSemaphore extends Semaphore {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        /**
         * Create a new semaphore with 0 permits.
         */
        ResizeableSemaphore() {
            super(0);
        }

        /*
         * (non-Javadoc)
         *
         * @see java.util.concurrent.Semaphore#reducePermits(int)
         */
        @Override
        protected void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }
}
