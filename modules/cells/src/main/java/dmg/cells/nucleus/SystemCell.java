package dmg.cells.nucleus ;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import dmg.util.AuthorizedString;
import dmg.util.Gate;
import dmg.util.command.Command;
import dmg.util.logback.FilterShell;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;

import static org.dcache.util.ByteUnit.MiB;

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

    /* Released on OOM to increase the chance that the shutdown succeeds.
     */
    private byte[] _oomSafetyBuffer = new byte[MiB.toBytes(2)];

   private final CellShell   _cellShell ;
   private final CellNucleus _nucleus ;
   private int  _packetsReceived,
                _packetsAnswered,
                _packetsForwarded,
                _packetsReplied,
                _exceptionCounter;
   private final Runtime                _runtime = Runtime.getRuntime() ;
   private final Gate                   _shutdownLock = new Gate(false);

   private class TheKiller extends Thread {
      @Override
      public void run(){
         _log.info("Running shutdown sequence");
         kill() ;
         _log.info("Kill done, waiting for shutdown lock");
         _shutdownLock.check() ;
         _log.info("Killer done");
      }
   }

    public static SystemCell create(String cellDomainName, CuratorFramework curatorFramework)
    {
        CellNucleus.initCellGlue(cellDomainName, curatorFramework);
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
        _shutdownLock.open();
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
        shutdownCells(nonSystem, System.currentTimeMillis() + 5000);

        _log.info("Will try to shutdown remaining cells {}", system);
        shutdownCells(system, System.currentTimeMillis() + 4000);
    }

    /**
     * Shuts down named cells. The method will block until the cells
     * are dead or until a timeout has occurred.
     *
     * @param cells List of names of cells to kill.
     * @param deadline Time in milliseconds since the epoch to wait for a cell to die.
     */
    private void shutdownCells(List<String> cells, long deadline)
    {
       for (String cellName: cells) {
           try {
               _nucleus.kill(cellName);
           } catch (IllegalArgumentException e) {
               _log.trace("Problem killing : {} -> {}", cellName, e.getMessage());
           }
       }

       for (String cellName: cells) {
           try {
               if (_nucleus.join(cellName, Math.max(1, deadline - System.currentTimeMillis()))) {
                   _log.info("Killed {}", cellName);
               } else {
                   _log.warn("Timeout waiting for {}", cellName);
                   CellNucleus.listThreadGroupOf(cellName);
                   break;
               }
           } catch (InterruptedException e) {
               _log.warn("Problem killing : {} -> {}",
                         cellName, e.getMessage());
               break;
           }
       }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.append(" CellDomainName   = ").println(getCellDomainName());
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
        _log.info( "Message arrived : "+msg ) ;
        _packetsReceived ++ ;
        if( msg.isReply() ){
            _log.warn("Seems to a bounce : "+msg);
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
           _log.info( "Command(p="+as.getAuthorizedPrincipal()+") : "+command ) ;
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
            _oomSafetyBuffer = null;
            kill();
            _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FATAL_JVM_ERROR,
                                                    getCellDomainName(),
                                                    getCellName()),
                       "Restarting due to fatal JVM error", e);
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
