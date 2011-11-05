package org.dcache.util;

import java.lang.Thread.UncaughtExceptionHandler;

/**
 *  A FireAndForgetTask is a Task that will catch any Throwable that the
 *  Runnable may produce and forward this to the Thread's UncaughtExceptionHandler.
 *  This is needed as the concurrency package's Executors have a habit of wrapping
 *  tasks within Future objects, which catch Exceptions so they can be reported
 *  when some Thread runs the get method of the Future.
 *  <p>
 *  This is problematic when one wishes to submit a request without recording the
 *  resulting Future since any Exception is caught without ever being reported.
 *  <p>
 *  This Class will catch any Throwable and report it to the running Thread's
 *  UncaughtExceptionHandler, thus ensuring that errors are propagated as
 *  expected.
 */
public class FireAndForgetTask implements Runnable {
    private final Runnable _inner;

    public FireAndForgetTask(Runnable r)
    {
        _inner = r;
    }

    @Override
    public void run()
    {
        try {
            _inner.run();
        } catch (Throwable e) {
            Thread thisThread = Thread.currentThread();
            UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
            ueh.uncaughtException( thisThread, e);
        }
    }
}
