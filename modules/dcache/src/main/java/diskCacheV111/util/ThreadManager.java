/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

package diskCacheV111.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.util.Args;

import org.dcache.util.FireAndForgetTask;

/**ThreadManager Cell.<br/>
   * This Cell provides threads to processes running the same domain. It is meant to help limit the total number
   * of threads running in the jvm.
   * @see ThreadPool
   **/
 public class ThreadManager extends CellAdapter implements ThreadPool, ThreadFactory {

     private final static Logger _log =
         LoggerFactory.getLogger(ThreadManager.class);

  /** Arguments specified in .batch file **/
  private Args _opt;

  /** Thread pool on which cells can execute runnables **/
  //private final ExecutorService executor;
  private static ThreadPoolTimedExecutor executor;

  /** Number of simultaneous requests to be handled. **/
  public static int THREAD_COUNT = 10;

  /** Starts a timing thread for each executing request and cancels it upon timeout. **/
  ScheduledExecutorService delaychecker;

  /** Elapsed time in seconds after which an authentication request is canceled.
   *  Includes both the time on the queue and the time for actual request processing. **/
  public static int DELAY_CANCEL_TIME = 15;

  /** Cancel time in milliseconds **/
  private static long toolong = 1000*DELAY_CANCEL_TIME;

  /** The singleton instance **/
  private static ThreadManager threadmanager;

  /** Count threads created **/
  private static int num_threads;


  /** Reads input parametes from batch file and initializes thread pools. **/
  public ThreadManager( String name , String args )  throws Exception {

    super( name , args , false ) ;

    //useInterpreter( true ) ;
    //addCommandListener(this);

    _opt = getArgs() ;

    try{

        /**
          *  USAGE :
          *              -num-threads=THREAD_COUNT
          *              -thread-timeout=DELAY_CANCEL_TIME
          *
         */

      THREAD_COUNT = setParam("num-threads", THREAD_COUNT);
      DELAY_CANCEL_TIME = setParam("thread-timeout", DELAY_CANCEL_TIME);
      toolong = 1000*DELAY_CANCEL_TIME;

      _log.info(this.toString() + " starting with " + THREAD_COUNT + " threads and timeout " + DELAY_CANCEL_TIME);

      //authpool = Executors.newFixedThreadPool(THREAD_COUNT);
      executor =
          new ThreadPoolTimedExecutor(THREAD_COUNT,
                                      THREAD_COUNT,
                                      60,
                                      TimeUnit.SECONDS,
                                      new LinkedBlockingQueue<Runnable>(),
                                      this);

      delaychecker =
          Executors.newScheduledThreadPool(THREAD_COUNT, this);

      start() ;

      _log.info(this.toString() + " started");

    } catch( Exception iae ){
        _log.warn(this.toString() + " couldn't start due to " + iae, iae);
      start() ;
      kill() ;
      throw iae ;
    }

	  _log.info(" Constructor finished" ) ;
   }

  public static ThreadManager getInstance() {
    return threadmanager;
  }

  public static ThreadPoolExecutor getExecutor() {
    return executor;
  }

      public static void execute(FutureTimedTask task)
      {
          ThreadPoolExecutor executor = ThreadManager.getExecutor();
          if (executor == null) {
              new Thread(task).start();
          } else {
              executor.execute(new FireAndForgetTask(task));
          }
      }

      public static void execute(final Runnable runnable)
      {
          final CDC cdc = new CDC();
          Runnable wrapper = new Runnable() {
                  @Override
                  public void run()
                  {
                      try (CDC ignored = cdc.restore()) {
                          runnable.run();
                      }
                  }
              };

          ThreadPoolExecutor executor = ThreadManager.getExecutor();
          if (executor == null) {
              new Thread(wrapper).start();
          } else {
              executor.execute(new FireAndForgetTask(wrapper));
          }
      }

  /** Set a parameter according to option specified in .batch config file **/
  private String setParam(String name, String target) {
   if(target==null) {
       target = "";
   }
   String option = _opt.getOpt(name) ;
   if((option != null) && (option.length()>0)) {
       target = option;
   }
   _log.info("Using " + name + " : " + target);
   return target;
  }

  /** Set a parameter according to option specified in .batch config file **/
  private int setParam(String name, int target) {
   String option = _opt.getOpt(name) ;
   if( ( option != null ) && ( option.length() > 0 ) ) {
     try{ target = Integer.parseInt(option); } catch(NumberFormatException e) {}
   }
   _log.info("Using " + name + " : " + target);
   return target;
  }

  /** Set a parameter according to option specified in .batch config file **/
  private long setParam(String name, long target) {
   String option = _opt.getOpt(name) ;
   if( ( option != null ) && ( option.length() > 0 ) ) {
     try{ target = Integer.parseInt(option); } catch(NumberFormatException e) {}
   }
   _log.info("Using " + name + " : " + target);
   return target;
  }

  /**
   * Composes output for "help" command
   */
  public String ac_help( Args args )
  {
      return super.ac_info(args);
  }

  /**
   * Composes output for "info" command
   */
  @Override
  public String ac_info(Args args)
  {
    StringBuilder sb = new StringBuilder(super.ac_info(args));
    sb.append("ActiveCount=").append(executor.getActiveCount()).append("\n");
    sb.append("CompletedTaskCount=").append(executor.getCompletedTaskCount())
            .append("\n");
    sb.append("CorePoolSize=").append(executor.getCorePoolSize()).append("\n");
    sb.append("KeepAliveTime=")
            .append(executor.getKeepAliveTime(TimeUnit.SECONDS)).append("\n");
    sb.append("LargestPoolSize=").append(executor.getLargestPoolSize())
            .append("\n");
    sb.append("MaximumPoolSize=").append(executor.getMaximumPoolSize())
            .append("\n");
    sb.append("PoolSize=").append(executor.getPoolSize()).append("\n");
    sb.append("TaskCount=").append(executor.getTaskCount()).append("\n");
    sb.append("IsShutdown=").append(executor.isShutdown()).append("\n");
    sb.append("IsTerminated=").append(executor.isTerminated()).append("\n");
    sb.append("IsTerminating=").append(executor.isTerminating()).append("\n");
    return sb.toString();
  }

  /**
   * Sets the allowed number of threads. Same as set CorePoolSize.
  */
  public static final String hh_set_NumThreads = "<numthreads>" ;
  public static final String fh_set_NumThreads =
    " set NumThreads <numthreads>\n"+
    "        Sets the allowed number of threads. Same as set CorePoolSize.\n"+
    "\n";
  public String ac_set_NumThreads_$_1( Args args ) {
    return ac_set_CorePoolSize_$_1( args );
  }

  /**
   * Sets the timeout for all threads.
  */
  public static final String hh_set_DelayCancelTime = "<timeout in seconds>" ;
  public static final String fh_set_DelayCancelTime =
    " set DelayCancelTime <timeout in seconds>\n"+
    "        Sets the timeout for all threads.\n"+
    "\n";
  public String ac_set_DelayCancelTime_$_1( Args args ) {
    int time  = Integer.parseInt( args.argv(0) );
    DELAY_CANCEL_TIME = time;
    toolong = 1000*DELAY_CANCEL_TIME;
    return "CancelTime set to " + DELAY_CANCEL_TIME + " seconds" ;
  }

  /**
   * Sets the allowed number of threads. Same as set NumThreads.
  */
  public static final String hh_set_CorePoolSize = "<poolsize>" ;
  public static final String fh_set_CorePoolSize =
    " set CorePoolSize <poolsize>\n"+
    "        Sets the allowed number of threads. Same as set NumThreads.\n"+
    "\n";
  public String ac_set_CorePoolSize_$_1( Args args ) {
    int size  = Integer.parseInt( args.argv(0) );
    executor.setCorePoolSize(size);
    THREAD_COUNT = size;
    return "CorePoolSize set to " + THREAD_COUNT ;
  }

  /**
   * Sets the time that an idle thread is kept.
  */
  public static final String hh_set_KeepAliveTime = "<keeptime in seconds>" ;
  public static final String fh_set_KeepAliveTime =
    " set KeepAliveTime <keeptime in seconds>\n"+
    "        Sets the time that an idle thread is kept.\n"+
    "\n";
  public String ac_set_KeepAliveTime_$_1( Args args ) {
    int time  = Integer.parseInt( args.argv(0) );
    executor.setKeepAliveTime(time, TimeUnit.SECONDS);
    return "KeepAliveTime set to " + time + " seconds" ;
  }

  /**
   * is called if user types 'info'
  */
  @Override
  public void getInfo( PrintWriter pw ){
     super.getInfo(pw);
     pw.println("ThreadManager");
     //pw.println(" admin_email :  " + _admin_email);
   }

   /**
    * This method is called from finalize() in the CellAdapter
    * super Class to clean the actions made from this Cell.
    * It stops the Thread created.
    */
   @Override
   public void cleanUp() {
       if (executor != null) {
           executor.shutdownNow();
       }
       if (delaychecker != null) {
           delaychecker.shutdownNow();
       }
   }

  /**
   * This method is invoked when a message arrives to this Cell.
   * The message is placed on the queue of the thread pool.
   * The sender of the message should block, waiting for the response.
   * @param msg CellMessage
  */
  @Override
  public synchronized void messageArrived( CellMessage msg ) {
     //if(msg.getMessageObject() instanceof DNInfo) {
     //  AuthFQANRunner arunner = new AuthFQANRunner(msg);
     //  FutureTimedTask authtask = new FutureTimedTask(arunner, null, System.currentTimeMillis());
     //  authpool.execute(authtask);
     //}
  }


  public synchronized void Message( String msg1, String msg2 ) {
    _log.info("Message received");
  }

  @Override
  public synchronized Thread newThread( Runnable target ){
    num_threads++;
    return newThread( target , "ThreadManager-" + num_threads ) ;
  }

  public Thread newThread( Runnable target , String name ){
    return getNucleus().newThread( target, name );
  }

  /**
   * Allows for skipping the normal thread processsing. Used in the case where a timeout
   * occurs before the thread is even invoked. A null message may still be returned, allowing
   * the calling thread to unblock.
   */
  public interface Skippable {
    public void setSkipProcessing(boolean skip);
  }

  /**
   * Extension of ThreadPoolExecutor to allow tasks to be terminated if a timeout occurs.
   * @see ThreadPoolExecutor
   */
  public class ThreadPoolTimedExecutor extends ThreadPoolExecutor {

    public ThreadPoolTimedExecutor(int corePoolSize,
                          int maximumPoolSize,
                          long keepAliveTime,
                          TimeUnit unit,
                          BlockingQueue<Runnable> workQueue,
                          ThreadFactory tfactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, tfactory);
    }

    /**
     * Start a second, timed task which will terminate the task if it has not finished.
     * If the timeout has already been reached, allows the task to run in abbreviated form.
     * The abbreviated form allows the task to immediately unblock other processes which may be waiting on it.
     * @param t
     * @param r
     */
    @Override
    public void beforeExecute(Thread t, Runnable r) {
      if(r instanceof TimedFuture) {
        TimedFuture timedtask = (TimedFuture) r;
        long now = System.currentTimeMillis();
        long then = timedtask.getCreateTime();
        long timeleft = toolong - (now - then);

        if(timeleft < 0) {
          timedtask.abbreviateTask(true);
        } else {
          TaskCanceller timerrunner = new TaskCanceller(timedtask);
          ScheduledFuture timer = delaychecker.schedule(timerrunner, timeleft, TimeUnit.MILLISECONDS);
          timedtask.setTimer(timer);
        }
      }
      super.beforeExecute(t, r);
    }

    /**
     * Terminate the timer task for the case where the task finished before the timeout.
     * @param r
     * @param t
     */
    @Override
    public void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      if(r instanceof TimedFuture) {
        TimedFuture timedtask = (TimedFuture) r;
        timedtask.cancelTimer();
      }
    }

  }


  /**
   * Interface allowing for tasks to be timed.
   * @see diskCacheV111.util.ThreadManager.FutureTimedTask
   */
  public interface TimedFuture<V> extends Future<V> {
    public long getCreateTime();
    public void setTimer(Future<V> timer);
    public void cancelTimer();
    public void abbreviateTask(boolean abbreviate);
  }

  /**
   * Runnable which cancels a task which has timed out.
   * Will generall have no effect on tasks which are blocking
   * on IO (unless NIO is being used). In that case, the specifice
   * IO sockets themselves will need to have their own timeouts.
   */
  public class TaskCanceller implements Runnable {
    private Future task;

    public TaskCanceller(Future task) {
      this.task = task;
    }

    public Future getFuture() {
      return task;
    }

    @Override
    public void run () {
      task.cancel(true);
    }

  }

   @Override
   public void invokeLater( Runnable runner , String name )
       throws IllegalArgumentException {}

    @Override
    public int getCurrentThreadCount() { return 0;}
    @Override
    public int getMaxThreadCount() {
        return ThreadManager.getExecutor().getMaximumPoolSize();
    }
    @Override
    public int getWaitingThreadCount() {return 0;}

    @Override
    public void setMaxThreadCount( int maxThreadCount )
        throws IllegalArgumentException {

        ThreadManager.THREAD_COUNT = maxThreadCount;
        ThreadManager.getExecutor().setCorePoolSize(THREAD_COUNT);
        ThreadManager.getExecutor().setMaximumPoolSize(THREAD_COUNT);
    }
}
