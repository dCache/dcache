package org.dcache.webadmin.view.beans;

import java.io.Serializable;

/**
 *
 * @author jans
 */
public class PoolRequestQueue implements Serializable {

    private int _active;
    private int _max;
    private int _queued;

    public PoolRequestQueue() {
    }

    public PoolRequestQueue(int active, int max, int queued) {
        _active = active;
        _max = max;
        _queued = queued;
    }

    public int getActive() {
        return _active;
    }

    public void setActive(int active) {
        _active = active;
    }

    public int getMax() {
        return _max;
    }

    public void setMax(int max) {
        _max = max;
    }

    public int getQueued() {
        return _queued;
    }

    public void setQueued(int queued) {
        _queued = queued;
    }

    public void addQueue(PoolRequestQueue queueToAdd) {
        if (queueToAdd == null) {
            throw new IllegalArgumentException();
        }
        _active = _active + queueToAdd.getActive();
        _max = _max + queueToAdd.getMax();
        _queued = _queued + queueToAdd.getQueued();
    }
}
