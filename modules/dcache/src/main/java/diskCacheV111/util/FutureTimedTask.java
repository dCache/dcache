// $Id: FutureTimedTask.java,v 1.2 2006-09-25 15:39:13 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.1.2.1  2006/09/20 17:39:06  tdh
// Task to be called by TaskManager. Extends FutureTask to allow tasks to be cancelled due to timeout.
//
//

package diskCacheV111.util;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import dmg.cells.nucleus.CDC;

/**
   * Extends FutureTask to allow tasks to be cancelled due to timeout.
   * @see FutureTask
   */
  public class FutureTimedTask<V> extends FutureTask<V> implements ThreadManager.TimedFuture<V> {

    private Callable<V> callable;
    private Runnable runnable;
    private Future<V> timer;
    private long createtime;
    private CDC cdc;

    public FutureTimedTask(Callable<V> callable, long createtime) {
      super(callable);
      this.callable = callable;
      this.createtime = createtime;
      this.cdc = new CDC();
    }

    public FutureTimedTask(Runnable runnable, V result, long createtime) {
      super(runnable, result);
      this.runnable = runnable;
      this.createtime = createtime;
      this.cdc = new CDC();
    }
    public Callable<V> getCallable() {
      return callable;
    }

    public Runnable getRunnable() {
      return runnable;
    }

    /**
     * Task which will terminate this task upon timeout.
     * @param timer
     */
    @Override
    public void setTimer(Future<V> timer) {
      this.timer = timer;
    }

    public Future<V> getTimer() {
      return timer;
    }

    /**
     * When this task finishes on time, its timer should be terminated.
     */
    @Override
    public void cancelTimer() {
      if(timer!=null) {
          timer.cancel(true);
      }
    }

    @Override
    public long getCreateTime() {
      return createtime;
    }

    /**
     * When a task times out on the queue before it is even executed,
     * it may need to run in an abbreviated form to unblock processes
     * which may be waiting on it.
     * @param abbreviate
     */
    @Override
    public void abbreviateTask(boolean abbreviate) {
      if(callable instanceof ThreadManager.Skippable) {
        ((ThreadManager.Skippable) callable).setSkipProcessing(abbreviate);
      }
      if(runnable instanceof ThreadManager.Skippable) {
        ((ThreadManager.Skippable) runnable).setSkipProcessing(abbreviate);
      }
    }

      @Override
      public void run()
      {
          try (CDC ignored = cdc.restore()) {
              super.run();
          }
      }

      @Override
      protected boolean runAndReset()
      {
          try (CDC ignored = cdc.restore()) {
              return super.runAndReset();
          }
      }
  }
