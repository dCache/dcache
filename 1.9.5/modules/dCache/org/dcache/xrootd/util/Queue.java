package org.dcache.xrootd.util;

import java.util.Vector;

/**
 *
 *
 * @author Patrick Fuhrmann
 * @author Martin Radicke
 * @version 0.2, 16 Jan 2005
 */

public class Queue {

    private Vector _v = new Vector();
    private long maxEntries;


    /**
     * default queue with 'unlimited' size
     *
     */
    public Queue() {
        this( Long.MAX_VALUE );
    }

    /**
     * queue with limited size
     *
     * @param max maximum number of queue entries
     */
    public Queue(long max) {
        if (max < 1L)
            throw new IllegalArgumentException("maximum number of queue entries must be at least 0");

        maxEntries = max;
    }

    public synchronized void push(Object o) throws InterruptedException {

        while (_v.size() >= maxEntries) {

            wait();

        }

        _v.add(o);
        notifyAll();

        return;
    }

    public synchronized Object pop() throws InterruptedException {

        while (_v.isEmpty()) {

            wait();

        }

        notifyAll();
        return _v.remove(0);

    }

    public synchronized void push(Object o, int timeout) throws InterruptedException {

        while (_v.size() >= maxEntries) {

            wait(timeout);

        }

        _v.add(o);
        notifyAll();

        return;
    }

    public synchronized Object pop(int timeout) throws InterruptedException {

        while (_v.isEmpty()) {

            wait(timeout);

        }

        notifyAll();
        return _v.remove(0);

    }

    public synchronized Object spy() {
        return _v.firstElement();
    }

    public synchronized int size() {
        return _v.size();
    }

    public synchronized Object[] dump() {
        return _v.toArray();
    }

}
