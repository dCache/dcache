package org.dcache.services.info.gathers;

import java.util.Date;

/**
 * Implements a skeleton Schedulable Class that is triggered periodically.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SkelPeriodicActivity implements Schedulable
{
    /** This activity's period, in seconds */
    private long _period;
    private Date _nextTrigger;

    /**
     * Create a new periodic activity, with a random initial offset.
     * @param period how often the trigger() should happen, in seconds.
     */
    public SkelPeriodicActivity(long period)
    {
        _period = period;
        _nextTrigger = new Date((long)(System.currentTimeMillis() + Math.random() * _period * 1000));
    }

    @Override
    public Date shouldNextBeTriggered()
    {
        return new Date(_nextTrigger.getTime());
    }

    @Override
    public void trigger()
    {
        _nextTrigger.setTime(System.currentTimeMillis() + _period * 1000);
    }


    /**
     * Return the recommended metric lifetime.  We assume a message might be lost and
     * 50% jitter how long the reply message will be received.
     * @return the metric period, in seconds.
     */
    protected long metricLifetime()
    {
        return (long) (_period * 2.5);
    }
}
