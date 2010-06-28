package org.dcache.webadmin.model.businessobjects;

import java.io.Serializable;

/**
 * simple container for moverqueues
 * @author jans
 */
public class MoverQueue implements Serializable {

    private int _active;
    private int _max;
    private int _queued;

    public MoverQueue() {
    }

    public MoverQueue(int active, int max, int queued) {
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

    @Override
    public int hashCode() {
        return _active ^ _max ^ _queued;
    }

    @Override
    public boolean equals(Object testObject) {
        if (this == testObject) {
            return true;
        }

        if (!(testObject instanceof MoverQueue)) {
            return false;
        }

        MoverQueue otherQueue = (MoverQueue) testObject;

        if (otherQueue._active != _active) {
            return false;
        }

        if (otherQueue._max != _max) {
            return false;
        }

        if (otherQueue._queued != _queued) {
            return false;
        }

        return true;
    }
}
