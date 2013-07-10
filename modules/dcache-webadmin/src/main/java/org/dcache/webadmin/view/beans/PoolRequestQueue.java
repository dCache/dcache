package org.dcache.webadmin.view.beans;

import java.io.Serializable;

/**
 *
 * @author jans
 */
public class PoolRequestQueue implements Serializable {

    private static final long serialVersionUID = 6998690438050477067L;
    private String _name;
    private int _active;
    private int _max;
    private int _queued;

    public PoolRequestQueue() {
    }

    public PoolRequestQueue(String name, int active, int max, int queued) {
        _name = name;
        _active = active;
        _max = max;
        _queued = queued;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
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

    /*
     * conveniance method to be able to calculate a total out of multiple queues
     */
    public void addQueue(PoolRequestQueue queueToAdd) {
        if (queueToAdd == null) {
            throw new IllegalArgumentException();
        }
        _active = _active + queueToAdd.getActive();
        _max = _max + queueToAdd.getMax();
        _queued = _queued + queueToAdd.getQueued();
    }
}
