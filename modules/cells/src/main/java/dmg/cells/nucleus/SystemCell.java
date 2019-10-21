package dmg.cells.nucleus ;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.remoting.RemoteProxyFailureException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import dmg.util.AuthorizedString;
import dmg.util.command.Command;
import dmg.util.logback.FilterShell;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;

import static java.util.stream.Collectors.toMap;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      SystemCell
    extends    CellAdapter
    implements Thread.UncaughtExceptionHandler
{
    private static final Logger _log = LoggerFactory.getLogger(SystemCell.class);

   private final CellShell   _cellShell ;
   private final CellNucleus _nucleus ;
   private int  _packetsReceived,
                _packetsAnswered,
                _packetsForwarded,
                _packetsReplied,
                _exceptionCounter;
   private final Runtime                _runtime = Runtime.getRuntime() ;
   private final Semaphore              _shutdownLock = new Semaphore(0);

   private class TheKiller extends Thread {
      @Override
      public void run(){
         _log.info("Running shutdown sequence");
         kill() ;
         _log.info("Kill done, waiting for shutdown lock");
         try{_shutdownLock.acquire(); } catch (Exception ee) {}
         _log.info("Killer done");
      }
   }

    public static SystemCell create(String cellDomainName,
            CuratorFramework curatorFramework, Optional<String> zone, SerializationHandler.Serializer serializer)
    {
        CellNucleus.initCellGlue(cellDomainName, curatorFramework, zone, serializer);
        return new SystemCell();
    }

    protected SystemCell()
    {
        super("System", "System", "");
        _nucleus = getNucleus();
        _cellShell = new CellShell(getNucleus());
    }

    @Override
    protected void starting()
    {
        /* We start the curator here to get the right context for the curator threads.
         */
        CellNucleus.startCurator();

        _cellShell.addCommandListener(this);
        _cellShell.addCommandListener(new LogbackShell());
        _cellShell.addCommandListener(new FilterShell(_nucleus.getLoggingThresholds()));
        _cellShell.addCommandListener(_cellShell.new HelpCommands());
        useInterpreter(false);

        _runtime.addShutdownHook(new TheKiller());
    }

    @Override
    protected void started()
    {
        Thread.setDefaultUncaughtExceptionHandler(this);
    }

    @Override
    public void stopped()
    {
        shutdownSystem();
        CellNucleus.shutdownCellGlue();
        _log.info("Opening shutdown lock");
        _shutdownLock.release();
        System.exit(0);
    }

    //
   // interface from Cell
   //
   public String toString(){
      long fm = _runtime.freeMemory() ;
      long tm = _runtime.totalMemory() ;
      return  getCellDomainName()+
              ":IOrec="+_packetsReceived+
              ";IOexc="+_exceptionCounter+
              ";MEM="+(tm-fm) ;
   }

    @Command(name = "get hostname", hint = "show this dCache-domain hostname",
            description = "Returns the hostname of the computer this (dCache) " +
                    "domain is running at. The hostname returned can be either " +
                    "the fully qualified domain name for this IP address " +
                    "or just 'localhost', if the local host name could not" +
                    " be resolved into an address.")
    public class GetHostnameCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            try {
                return InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException ex) {
                return "localhost";
            }
        }
    }

    private void shutdownSystem()
    {
        List<String> names = _nucleus.getCellNames();
        List<String> nonSystem = new ArrayList<>(names.size());
        List<String> system = new ArrayList<>(names.size());

        for (String name: names) {
            CellInfo info = _nucleus.getCellInfo(name);
            if (info == null) {
                continue;
            }
            String cellName = info.getCellName();
            if (cellName.equals("System")) {
                // Don't kill the system cell
            } else if (info.getCellType().equals("System")) {
                system.add(cellName);
            } else {
                nonSystem.add(cellName);
            }
        }

        _log.info("Will try to shutdown non-system cells {}", nonSystem);
        shutdownCells(nonSystem, 5000, 10000);

        _log.info("Will try to shutdown remaining cells {}", system);
        shutdownCells(system, 5000, 10000);
    }

    /**
     * Shuts down named cells. The method will block until the cells
     * are dead or until a timeout has occurred.
     *
     * @param cells List of names of cells to kill.
     * @param softTimeout Timeout in milliseconds to wait until we log the cells we are waiting for
     * @param hardTimeout Timeout in milliseconds to wait until we log stack traces and give up
     */
    private void shutdownCells(List<String> cells, long softTimeout, long hardTimeout)
    {
        /* We log the completion of cell shutdown from a listener.
         */
        final long start = System.currentTimeMillis();
        Function<String, Runnable> listeners =
                name -> () -> {
                    long time = System.currentTimeMillis() - start;
                    if (time > softTimeout) {
                        _log.warn("Killed {} in {} ms", name, time);
                    } else {
                        _log.info("Killed {}", name);
                    }
                };

        /* Kill all the cells.
         */
        Map<String, CompletableFuture<?>> futures = cells.stream().collect(toMap(name -> name, _nucleus::kill));

        /* And attach the listener.
         */
        futures.forEach((name, future) -> future.thenRunAsync(listeners.apply(name), MoreExecutors.directExecutor()));

        /* Now wait.
         */
        try {
            CompletableFuture<Void>[]  futuresAsArray = futures.values().toArray(new CompletableFuture[0]);
            try {
                CompletableFuture.allOf(futuresAsArray).get(softTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                futures.forEach((name, future) -> {
                    if (!future.isDone()) {
                        _log.warn("Still waiting for {} to shut down.", name);
                    }
                });
                CompletableFuture.allOf(futuresAsArray).get(hardTimeout - softTimeout, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            futures.forEach((name, future) -> {
                if (!future.isDone()) {
                    CellNucleus.listThreadGroupOf(name);
                }
            });
            CellNucleus.listKillerThreadGroup();
        } catch (ExecutionException e) {
            _log.error("Unexpected exception during shutdown.", e.getCause());
        }
}

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.append(" CellDomainName   = ").println(getCellDomainName());
        pw.append(" Zone = ").println(_nucleus.getZone().orElse("(none)"));
        pw.append(" Message payload serializer = ").println(_nucleus.getMsgSerialization());
        pw.format(" I/O rcv=%d;asw=%d;frw=%d;rpy=%d;exc=%d\n",
                  _packetsReceived, _packetsAnswered, _packetsForwarded,
                  _packetsReplied, _exceptionCounter);
        long fm = _runtime.freeMemory();
        long tm = _runtime.totalMemory();

        pw.format(" Memory : tot=%d;free=%d;used=%d\n", tm, fm, tm - fm);
        pw.println(" Cells (Threads)");
        for (String name: _nucleus.getCellNames()) {
            pw.append(" ").append(name).append("(");
            Thread[] threads = _nucleus.getThreads(name);
            if (threads != null) {
                boolean first = true;
                for (Thread thread: threads) {
                    pw.print(thread.getName());
                    if (first) {
                        first = false;
                    } else {
                        pw.print(",");
                    }
                }
            }
            pw.println(")");
        }
    }

   @Override
   public void messageToForward( CellMessage msg ){
        try{
           sendMessage( msg ) ;
           _packetsForwarded ++ ;
        }catch( RuntimeException eee ){
           _exceptionCounter ++ ;
        }
   }

   @Override
   public void messageArrived( CellMessage msg ){
        _log.info( "Message arrived : {}", msg ) ;
        _packetsReceived ++ ;
        if( msg.isReply() ){
            _log.warn("Seems to a bounce : {}", msg);
            return ;
        }
        Object obj  = msg.getMessageObject() ;
        Serializable reply;

        if(obj instanceof String) {
           String command = (String) obj;
           if (command.isEmpty()) {
               return;
           }
           _log.info("Command: {}", command);
           if (command.equals("xyzzy")) {
               reply = "Nothing happens.";
           } else {
               reply = _cellShell.objectCommand2(command);
           }
        }else if( obj instanceof AuthorizedString ){
           AuthorizedString as = (AuthorizedString)obj ;
           String command = as.toString() ;
           if( command.length() < 1 ) {
               return;
           }
           _log.info( "Command(p={}) : {}", as.getAuthorizedPrincipal(), command ) ;
           reply = _cellShell.objectCommand2( command ) ;
        } else {
            return;
        }

       _log.debug("Reply : {}", reply);
       _packetsAnswered++;

        try {
            if (reply instanceof Reply) {
                ((Reply) reply).deliver(this, msg);
            } else {
                msg.revertDirection();
                msg.setMessageObject(reply);
                sendMessage(msg);
                _log.debug("Sending : {}", msg);
            }
            _packetsReplied++;
        }catch( RuntimeException e ){
            _exceptionCounter ++ ;
        }
   }

    @Override
    public void uncaughtException(Thread t, Throwable e)
    {
        /* In case of fatal errors we shut down. The wrapper script
         * will restart the domain. Notice that there is no guarantee
         * that the fatal error will not reoccur during shutdown and
         * in that case the shutdown may fail. We may want to consider
         * refining the shutdown logic such that in recovers if the
         * fatal error reoccurs.
         */
        if (e instanceof VirtualMachineError) {
            kill();
            _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FATAL_JVM_ERROR,
                                                    getCellDomainName(),
                                                    getCellName()),
                       "Restarting due to fatal JVM error: {}", e.toString());
            return;
        }

        /*
         *  The RemotePoolMonitor wraps interrupted exceptions in a
         *  runtime exception.  These should not cause a stack trace to
         *  be printed.
         */
        if (e instanceof RemoteProxyFailureException &&
                        e.getCause() instanceof InterruptedException) {
            _log.warn("{} interrupted.", t.getName());
            return;
        }

        Throwable root = Throwables.getRootCause(e);
        if (root instanceof FileNotFoundException) {
            if (root.getMessage().contains("Too many open files")) {
                _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.OUT_OF_FILE_DESCRIPTORS,
                                                        getCellDomainName(),
                                                        getCellName()),
                           "Uncaught exception in thread " + t.getName(),
                           e);
                return;
            }
        }

        _log.error("Uncaught exception in thread " + t.getName(), e);
    }
}
