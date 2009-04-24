package org.dcache.acl.util.thread;

/**
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class ThreadTerminator {
    // Try to kill; return true if known to be dead
    public static boolean terminate(Thread t, long maxWaitToDie) {

        if ( t.isAlive() == false )
            return true; // already dead

        // phase 1 -- graceful cancellation
        t.interrupt();
        try {
            t.join(maxWaitToDie);
        } catch (InterruptedException e) {
        } // ignore

        if ( t.isAlive() == false )
            return true; // success

        if ( t.isAlive() )
            return true;

        // phase 2 -- minimize damage
        t.setPriority(Thread.MIN_PRIORITY);
        return false;
    }

}