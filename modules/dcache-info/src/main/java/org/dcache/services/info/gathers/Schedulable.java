package org.dcache.services.info.gathers;

import java.util.Date;

/**
 * The generic interface that allows schedulable activity.  The activity should
 * be characterised by short bursts of activity with gaps in between.  The
 * activity may be periodic, but this is not a requirement.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface Schedulable
{
    /**
     *   This method should effect some data-gathering activity; that is,
     *   do something that should result in information being added or
     *   updated within State.
     *   <p>
     *   Implementations of this interface should make sure this
     *   method returns <i>reasonably</i> quickly.  We don't define
     *   "reasonably" precisely, but it should return within a
     *   duration &lt;&lt; the average period between successive requests
     *   for any registered DataGatheringActivity.
     *   <p>
     *   In practise, since this method must return quickly, it should always use
     *   send CellMessages to the target Cell requesting information and must not
     *   wait for the reply message.
     */
    void trigger();

    /**
     * Provide the time when an object who's class implements this interface should
     * next be triggered.
     * @return requested next trigger.
     */
    Date shouldNextBeTriggered();
}
