package org.dcache.services.info.gathers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.cells.nucleus.UOID;

import org.dcache.util.NDC;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateUpdateManager;

import static com.google.common.base.Preconditions.checkState;

/**
 * This thread is responsible for scheduling various data-gathering activity.
 * Multiple DataGatheringActivity instances can be registered, each will operate
 * independently.  The frequency at which they trigger, or even whether they are
 * periodic, is completely under the control of the DGA.
 * <p>
 * These DataGatheringActivities can (in principle) do anything when
 * triggered, but will typically send one or more messages to dCache.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */

public class DataGatheringScheduler implements Runnable, EnvironmentAware
{
    private static final long FIVE_MINUTES = 5*60*1000;
    private static final Logger LOGGER_SCHED = LoggerFactory.getLogger(DataGatheringScheduler.class);
    private static final Logger LOGGER_RA = LoggerFactory.getLogger(RegisteredActivity.class);

    private boolean _timeToQuit;
    private final List<RegisteredActivity> _activity = new ArrayList<>();
    private Map<String,Object> _environment;
    private Iterable<DgaFactoryService> _factories;

    private StateUpdateManager _sum;
    private StateExhibitor _exhibitor;
    private MessageSender _sender;
    private MessageMetadataRepository<UOID> _repository;
    private Thread _thread;


    /**
     * Class holding a periodically repeated DataGatheringActivity
     * @author Paul Millar <paul.millar@desy.de>
     */
    private static class RegisteredActivity
    {
        /** Min. delay (in ms). We prevent Schedulables from triggering more frequently than this */
        private static final long MINIMUM_DGA_DELAY = 50;

        private final Schedulable _dga;

        /** The delay until this DataGatheringActivity should be next triggered */
        private Date _nextTriggered;

        /** Whether we should include this activity when scheduling next activity */
        private boolean _enabled = true;

        /**
         * Create a new PeriodicActvity, with specified DataGatheringActivity, that
         * is triggered with a fixed period.  The initial delay is a randomly chosen
         * fraction of the period.
         * @param dga the DataGatheringActivity to be triggered periodically
         * @param period the period between successive triggering in milliseconds.
         */
        RegisteredActivity(Schedulable dga)
        {
            _dga = dga;
            updateNextTrigger();
        }


        /**
         * Try to make sure we don't hit the system with lots of queries at the same
         * time
         * @param period
         */
        private void updateNextTrigger()
        {
            Date nextTrigger = _dga.shouldNextBeTriggered();

            if (nextTrigger == null) {
                LOGGER_RA.error("registered dga returned null Date");
                nextTrigger = new Date(System.currentTimeMillis() + FIVE_MINUTES);
            } else {
                // Safety!  Check we wont trigger too quickly
                if (nextTrigger.getTime() - System.currentTimeMillis() <  MINIMUM_DGA_DELAY) {
                    LOGGER_RA.warn("DGA {} triggering too quickly ({}ms): engaging safety.",
                            _dga, nextTrigger.getTime() - System.currentTimeMillis());
                    nextTrigger = new Date (System.currentTimeMillis() + MINIMUM_DGA_DELAY);
                }
            }

            _nextTriggered = nextTrigger;
        }

        /**
         * Update this PeriodicActivity so it's trigger time is <i>now</i>.
         */
        public void shouldTriggerNow()
        {
            _nextTriggered = new Date();
        }

        /**
         * Check the status of this activity.  If the time has elapsed,
         * this will cause the DataGatheringActivity to be triggered
         * and the timer to be reset.
         * @return true if the DataGatheringActivity was triggered.
         */
        boolean checkAndTrigger(Date now)
        {
            if (!_enabled) {
                return false;
            }

            if (now.before(_nextTriggered)) {
                return false;
            }

            NDC.push(_dga.toString());
            _dga.trigger();
            NDC.pop();

            updateNextTrigger();
            return true;
        }

        /**
         * Calculate the duration until the event has triggered.
         * @return duration, in milliseconds, until event or zero if it
         * should have been triggered already.
         */
        long getDelay()
        {
            long delay = _nextTriggered.getTime() - System.currentTimeMillis();
            return delay > 0 ? delay : 0;
        }

        /**
         * Return the time this will be next triggered.
         * @return
         */
        long getNextTriggered()
        {
            return _nextTriggered.getTime();
        }

        boolean isEnabled()
        {
            return _enabled;
        }

        void disable()
        {
            _enabled = false;
        }

        /**
         * Enable a periodic activity.
         */
        void enable()
        {
            if (!_enabled) {
                _enabled = true;
                updateNextTrigger();
            }
        }

        /**
         * A human-understandable name for this DGA
         * @return the underlying DGA's name
         */
        @Override
        public String toString()
        {
            return _dga.toString();
        }

        /**
         * Render current status into a human-understandable form.
         * @return single-line String describing current status.
         */
        public String getStatus()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(this.toString());
            sb.append(" [");
            sb.append(_enabled ? "enabled" : "disabled");
            if (_enabled) {
                sb.append(String
                        .format(", next %1$.1fs", getDelay() / 1000.0));
            }
            sb.append("]");

            return sb.toString();
        }
    }

    public synchronized void start()
    {
        checkState(_thread == null, "DataGatheringScheduler already started");

        for (DgaFactoryService factory : _factories) {
            if (factory instanceof EnvironmentAware) {
                ((EnvironmentAware)factory).setEnvironment(_environment);
            }

            for (Schedulable dga : factory.createDgas(_exhibitor, _sender,
                    _sum, _repository)) {
                _activity.add(new RegisteredActivity(dga));
            }
        }

        _thread = new Thread(this);
        _thread.setName("DGA-Scheduler");
        _thread.start();
    }

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _environment = environment;
    }

    @Required
    public void setDgaFactories(Iterable<DgaFactoryService> factories)
    {
        _factories = factories;
    }

    @Required
    public void setStateUpdateManager(StateUpdateManager sum)
    {
        _sum = sum;
    }

    @Required
    public void setStateExhibitor(StateExhibitor exhibitor)
    {
        _exhibitor = exhibitor;
    }

    @Required
    public void setMessageSender(MessageSender sender)
    {
        _sender = sender;
    }

    @Required
    public void setMessageMetadataRepository(MessageMetadataRepository<UOID> repository)
    {
        _repository = repository;
    }

    /**
     * Main loop for this thread triggering DataGatheringActivity.
     */
    @Override
    public void run()
    {
        long delay;
        Date now = new Date();

        LOGGER_SCHED.debug("DGA Scheduler thread starting.");

        synchronized (_activity) {
            do {
                now.setTime(System.currentTimeMillis());

                for (RegisteredActivity pa : _activity) {
                    pa.checkAndTrigger(now);
                }

                delay = getWaitTimeout();

                try {
                    _activity.wait(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } while (!_timeToQuit);
        }

        LOGGER_SCHED.debug("DGA Scheduler thread shutting down.");
    }

    /**
     * Search through out list of activity and find the one that matches this name.
     * <p>
     * This method assumes that the current thread already owns the _allActivity
     * monitor
     * @param name the name of the activity to fine
     * @return the corresponding PeriodicActivity object, or null if not found.
     */
    private RegisteredActivity findActivity(String name)
    {
        RegisteredActivity foundPA = null;

        for (RegisteredActivity pa : _activity) {
            if (pa.toString().equals(name)) {
                foundPA = pa;
                break;
            }
        }

        return foundPA;
    }


    /**
     * Enable a data-gathering activity, based on a human-readable name.
     * @param name - name of the DGA.
     * @return null if successful or an error message if there was a problem.
     */
    public String enableActivity(String name)
    {
        RegisteredActivity pa;
        boolean haveEnabled = false;

        synchronized (_activity) {
            pa = findActivity(name);

            if (pa != null && !pa._enabled) {
                pa.enable();
                _activity.notify();
                haveEnabled = true;
            }
        }

        return haveEnabled ? null : pa == null ? "Unknown DGA " + name : "DGA " + name + " already enabled";
    }

    /**
     * Disabled a data-gathering activity, based on a human-readable name.
     * @param name - name of the DGA.
     * @return null if successful or an error message if there was a problem.
     */
    public String disableActivity(String name)
    {
        RegisteredActivity pa;
        boolean haveDisabled = false;

        synchronized (_activity) {
            pa = findActivity(name);

            if (pa != null && pa._enabled) {
                pa.disable();
                _activity.notify();
                haveDisabled = true;
            }
        }

        return haveDisabled ? null : pa == null ? "Unknown DGA " + name : "DGA " + name + " already disabled";
    }


    /**
     * Trigger a periodic activity right now.
     * @param name the PeriodicActivity to trigger
     * @return null if successful, an error message if there was a problem.
     */
    public String triggerActivity(String name)
    {
        RegisteredActivity pa;

        synchronized (_activity) {
            pa = findActivity(name);

            if (pa != null) {
                pa.shouldTriggerNow();
                _activity.notify();
            }
        }

        return pa != null ? null : "Unknown DGA " + name;
    }


    /**
     * Request that this thread sends no more requests
     * for data.
     */
    public void shutdown()
    {
        LOGGER_SCHED.debug("Requesting DGA Scheduler to shutdown.");
        synchronized (_activity) {
            _timeToQuit = true;
            _activity.notify();
        }
    }


    /**
     * Calculate the delay, in milliseconds, until the next
     * PeriodicActivity is to be triggered, or 0 if there is
     * no registered Schedulable objects.
     * <p>
     * <i>NB</i> we assume that the current thread has already obtained the monitor for
     * _allActivity!
     * @return delay, in milliseconds, until next trigger or zero if there
     * is no recorded delay.
     */
    private long getWaitTimeout()
    {
        long earliestTrig=0;

        synchronized (_activity) {

            for (RegisteredActivity thisPa : _activity) {

                if (!thisPa.isEnabled()) {
                    continue;
                }

                long thisTrig = thisPa.getNextTriggered();

                if (thisTrig < earliestTrig || earliestTrig == 0) {
                    earliestTrig = thisTrig;
                }
            }
        }

        long delay = 0;

        if (earliestTrig > 0) {
            delay = earliestTrig - System.currentTimeMillis();
            delay = delay < 1 ? 1 : delay; // enforce >1 to distinguish between "should trigger now" and "no registered activity".
        }

        return delay;
    }


    /**
     * Return a human-readable list of known activity.
     * @return
     */
    public List<String> listActivity()
    {
        List<String> activityList = new ArrayList<>();

        synchronized (_activity) {
            for (RegisteredActivity thisRa : _activity) {
                activityList.add(thisRa.getStatus());
            }
        }

        return activityList;
    }
}
