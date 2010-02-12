package dmg.cells.nucleus ;
import dmg.util.*;
import java.io.*;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      SystemCell
    extends    CellAdapter
    implements Runnable, Thread.UncaughtExceptionHandler
{
    private final static Logger _log = LoggerFactory.getLogger(SystemCell.class);

   private final CellShell   _cellShell ;
   private final CellNucleus _nucleus ;
   private int  _packetsReceived  = 0 ,
                _packetsAnswered  = 0 ,
                _packetsForwarded = 0 ,
                _packetsReplayed  = 0 ,
                _exceptionCounter = 0 ;
   private DomainInterruptHandler _interruptHandler = null ;
   private Thread                 _interruptThread  = null ;
   private long                   _interruptTimer   = 2000 ;
   private final Runtime                _runtime = Runtime.getRuntime() ;
   private final Gate                   _shutdownLock = new Gate(false);

   private class TheKiller extends Thread {
      public void run(){
         say("Running shutdown sequence");
         kill() ;
         say("Kill done, waiting for shutdown lock");
         _shutdownLock.check() ;
         say("Killer done");
      }
   }
   public SystemCell( String cellDomainName  ){
       super( cellDomainName ) ;

       _nucleus   = getNucleus() ;
       _cellShell = new CellShell( getNucleus() ) ;
       _cellShell.addCommandListener(new Log4jShell());
       useInterpreter( false ) ;

       _runtime.addShutdownHook( new TheKiller() ) ;

       Thread.setDefaultUncaughtExceptionHandler(this);
//       setPrintoutLevel(0xff);

//      addCellEventListener() ;
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
   public int  enableInterrupts( String handlerName ){
      Class handlerClass = null ;
      try{
          handlerClass = Class.forName( handlerName ) ;
      }catch( ClassNotFoundException cnfe ){
          esay( "Couldn't install interrupt handler ("+
                handlerName+") : "+cnfe ) ;
         return -1 ;
      }
      try{
          _interruptHandler = (DomainInterruptHandler)handlerClass.newInstance() ;
      }catch( Exception ee ){
          esay( "Couldn't install interrupt handler ("+
                handlerName+") : "+ee ) ;
          return -2 ;
      }
      _interruptThread = _nucleus.newThread( this ) ;
      _interruptThread.start() ;
      return 0 ;
   }
   public void run(){
       while( true ){
          try{
             Thread.sleep( _interruptTimer ) ;
             if( _interruptHandler.interruptPending() )break ;
          }catch( InterruptedException ie ){
             say( "Interrupt loop was interrupted" ) ;
             break ;
          }
       }
       say( "Interrupt loop stopped (shutting down system now)" ) ;
       kill() ;
   }
    private void shutdownSystem()
    {
        String [] names = _nucleus.getCellNames();
        List<String> nonSystem = new ArrayList<String>(names.length);
        List<String> system = new ArrayList<String>(names.length);

        for (int i = 0; i < names.length; i++) {
            CellInfo info = _nucleus.getCellInfo(names[i]);
            if (info == null) continue;
            String cellName = info.getCellName();
            if (cellName.equals("System")) {
                // Don't kill the system cell
            } else if (info.getCellType().equals("System")) {
                system.add(cellName);
            } else {
                nonSystem.add(cellName);
            }
        }

        say("Will try to shutdown non-system cells " + nonSystem);
        shutdownCells(nonSystem, 3000);

        say("Will try to shutdown remaining cells " + system);
        shutdownCells(system, 5000);
    }

    /**
     * Shuts downs named cells. The method will block until the cells
     * are dead or until a timeout has occurred.
     *
     * @param cells List of names of cells to kill.
     * @param timeout Time in milliseconds to wait for a cell to die.
     */
    private void shutdownCells(List<String> cells, long timeout)
    {
       for (String cellName : cells) {
           try {
               _nucleus.kill(cellName);
           } catch (IllegalArgumentException e) {
               say("Problem killing : " + cellName + " -> " + e.getMessage());
           }
       }

       for (String cellName : cells) {
           try {
               if (_nucleus.join(cellName, timeout)) {
                   say("Killed " + cellName);
               } else {
                   esay("Timeout waiting for " + cellName);
                   break;
               }
           } catch (InterruptedException e) {
               esay("Problem killing : " + cellName + " -> " + e.getMessage());
               break;
           }
       }
    }

   public void cleanUp(){
       shutdownSystem() ;
       say("Opening shutdown lock") ;
       _shutdownLock.open();
       System.exit(0) ;
   }
   public void getInfo( PrintWriter pw ){
      pw.println( " CellDomainName   = "+getCellDomainName() ) ;
      pw.print( " I/O rcv="+_packetsReceived ) ;
      pw.print( ";asw="+_packetsAnswered ) ;
      pw.print( ";frw="+_packetsForwarded ) ;
      pw.print( ";rpy="+_packetsReplayed ) ;
      pw.println( ";exc="+_exceptionCounter ) ;
      long fm = _runtime.freeMemory() ;
      long tm = _runtime.totalMemory() ;

      pw.println( " Memory : tot="+tm+";free="+fm+";used="+(tm-fm) ) ;
      pw.println( " Cells (Threads)" ) ;
      //
      // count the threads
      //
      String [] names = _nucleus.getCellNames() ;
      for( int i = 0 ; i < names.length ; i++ ){
         pw.print( " "+names[i]+"(" ) ;
         Thread [] threads = _nucleus.getThreads(names[i]) ;
         if( threads != null ){
           for( int j = 0 ; j < threads.length ; j++ )
             pw.print( threads[j].getName()+"," ) ;
         }
         pw.println(")");
      }
   }
   public void messageToForward( CellMessage msg ){
        msg.nextDestination() ;
        try{
           sendMessage( msg ) ;
           _packetsForwarded ++ ;
        }catch( Exception eee ){
           _exceptionCounter ++ ;
        }
   }
   public void messageArrived( CellMessage msg ){
        say( "Message arrived : "+msg ) ;
        _packetsReceived ++ ;
        if( msg.isReply() ){
            esay("Seems to a bounce : "+msg);
            return ;
         }
        Object obj  = msg.getMessageObject() ;
        if( obj instanceof String ){
           String command = (String)obj ;
           if( command.length() < 1 )return ;
           Object reply = null ;
           say( "Command : "+command ) ;
           reply = _cellShell.objectCommand2( command ) ;
           say( "Reply : "+reply ) ;
           msg.setMessageObject( reply ) ;
           _packetsAnswered ++ ;
        }else if( obj instanceof AuthorizedString ){
           AuthorizedString as = (AuthorizedString)obj ;
           String command = as.toString() ;
           if( command.length() < 1 )return ;
           Object reply = null ;
           say( "Command(p="+as.getAuthorizedPrincipal()+") : "+command ) ;
           reply = _cellShell.objectCommand2( command ) ;
           say( "Reply : "+reply ) ;
           msg.setMessageObject( reply ) ;
           _packetsAnswered ++ ;
        }else if( obj instanceof CommandRequestable ){
           CommandRequestable request = (CommandRequestable)obj ;
           Object reply = null ;
           try{
              say( "Command : "+request.getRequestCommand() ) ;
              reply = _cellShell.command( request ) ;
           }catch( CommandException cee ){
              reply = cee ;
           }
           say( "Reply : "+reply ) ;
           msg.setMessageObject( reply ) ;
           _packetsAnswered ++ ;
        }
        try{
           msg.revertDirection() ;
           sendMessage( msg ) ;
           say( "Sending : "+msg ) ;
           _packetsReplayed ++ ;
        }catch( Exception e ){
           _exceptionCounter ++ ;
        }
   }

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
            _log.error("Fatal JVM error", e);
            _log.error("Shutting down...");
            kill();
        }

        _log.error("Uncaught exception in thread " + t.getName(), e);
    }
}
