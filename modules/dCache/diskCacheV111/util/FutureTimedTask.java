// $Id: FutureTimedTask.java,v 1.2 2006-09-25 15:39:13 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.1.2.1  2006/09/20 17:39:06  tdh
// Task to be called by TaskManager. Extends FutureTask to allow tasks to be cancelled due to timeout.
//
//

package diskCacheV111.util;

import dmg.cells.nucleus.CDC;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
   * Extends FutureTask to allow tasks to be cancelled due to timeout.
   * @see java.util.concurrent.FutureTask
   */
  public class FutureTimedTask extends FutureTask implements ThreadManager.TimedFuture {

    private Callable callable;
    private Runnable runnable;
    private Future timer;
    private long createtime;
    private CDC cdc;

    public FutureTimedTask(Callable callable, long createtime) {
      super(callable);
      this.callable = callable;
      this.createtime = createtime;
      this.cdc = new CDC();
    }

    public FutureTimedTask(Runnable runnable, Object result, long createtime) {
      super(runnable, result);
      this.runnable = runnable;
      this.createtime = createtime;
      this.cdc = new CDC();
    }
    public Callable getCallable() {
      return callable;
    }

    public Runnable getRunnable() {
      return runnable;
    }

    /**
     * Task which will terminate this task upon timeout.
     * @param timer
     */
    public void setTimer(Future timer) {
      this.timer = timer;
    }

    public Future getTimer() {
      return timer;
    }

    /**
     * When this task finishes on time, its timer should be terminated.
     */
    public void cancelTimer() {
      if(timer!=null) timer.cancel(true);
    }

    public long getCreateTime() {
      return createtime;
    }

    /**
     * When a task times out on the queue before it is even executed,
     * it may need to run in an abbreviated form to unblock processes
     * which may be waiting on it.
     * @param abbreviate
     */
    public void abbreviateTask(boolean abbreviate) {
      if(callable instanceof ThreadManager.Skippable) {
        ((ThreadManager.Skippable) callable).setSkipProcessing(abbreviate);
      }
      if(runnable instanceof ThreadManager.Skippable) {
        ((ThreadManager.Skippable) runnable).setSkipProcessing(abbreviate);
      }
    }

      public void run()
      {
          cdc.restore();
          try {
              super.run();
          } finally {
              CDC.clear();
          }
      }

      protected boolean runAndReset()
      {
          cdc.restore();
          try {
              return super.runAndReset();
          } finally {
              CDC.clear();
          }
      }
  }