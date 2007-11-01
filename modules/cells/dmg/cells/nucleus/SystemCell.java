package dmg.cells.nucleus ;
import  dmg.util.* ;
import  java.io.* ;
/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      SystemCell 
       extends    CellAdapter 
       implements Runnable      {

   private CellShell   _cellShell = null ;
   private CellNucleus _nucleus   = null ;
   private int  _packetsReceived  = 0 ,
                _packetsAnswered  = 0 ,
                _packetsForwarded = 0 ,
                _packetsReplayed  = 0 ,
                _exceptionCounter = 0 ;
   private DomainInterruptHandler _interruptHandler = null ;
   private Thread                 _interruptThread  = null ;
   private long                   _interruptTimer   = 2000 ;
   private Runtime                _runtime = Runtime.getRuntime() ;
   private Gate                   _shutdownLock = new Gate(false);
   
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
       useInterpreter( false ) ;
       
       _runtime.addShutdownHook( new TheKiller() ) ;
       
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
       say( "Interrupt loop stopped (shuting down system now)" ) ;
       kill() ;
   }
   private void shutdownSystem(){
       String [] names = _nucleus.getCellNames() ;
       say( "Will try to shutdown non type=System cells ("+names.length+")" ) ;
       for( int i = 0 ; i < names.length ; i++ ){
          CellInfo info = _nucleus.getCellInfo( names[i] ) ;
          if( info == null )continue ;
          String cellName = info.getCellName() ;
          if( info.getCellType().equals( "System" ) ){
             say( "Not Killing "+cellName) ;
          }else{
             say( "Killing "+cellName) ;
             try{
                _nucleus.kill( cellName ) ;
             }catch(Exception ee){
                esay("Problem killing : "+cellName+" -> "+ee.getMessage());
             }
          }
       }
       try{ Thread.sleep(3000) ; }
       catch(Exception ee){}; 
       names = _nucleus.getCellNames() ;
       say( "Will try to shutdown rest (except System) ("+names.length+")" ) ;
       for( int i = 0 ; i < names.length ; i++ ){
          CellInfo info = _nucleus.getCellInfo( names[i] ) ;
          if( info == null )continue ;
          String cellName = info.getCellName() ;
          if( cellName.equals( "System" ) )continue ;
          say( "Killing (2) "+cellName) ;
          try{
             _nucleus.kill( cellName ) ;
          }catch(Exception ee){
             esay("Problem killing : "+cellName+" -> "+ee.getMessage());
          }
       }
       for( int i = 0 ; i < 5 ; i++ ){
           try{ Thread.sleep(1000) ; }
           catch(Exception ee){}; 
           names = _nucleus.getCellNames() ;
           if( ( names.length -1 ) == 0 ){
              say( "All cells are shut down now" ) ;
              break ;
           }
           StringBuffer sb = new StringBuffer() ;
           for( int l = 0 ; l < names.length ; l++ )
              sb.append(names[l]).append(",") ;
           say( "Still waiting for "+(names.length-1)+" cells("+sb.toString()+")" ) ;
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
        if( msg.isPersistent() ){
            esay("Seems to a bounce : "+msg);
            return ;
         }
        Object obj  = msg.getMessageObject() ;
        if( obj instanceof String ){
           String command = (String)obj ;
           if( command.length() < 1 )return ;
           Object reply = null ;
           try{
              say( "Command : "+command ) ;
              reply = _cellShell.objectCommand2( command ) ;
           }catch( CommandExitException cee ){
              reply = "Can't exit SystemShell" ;
           }
           say( "Reply : "+reply ) ;
           msg.setMessageObject( reply ) ;
           _packetsAnswered ++ ;
        }else if( obj instanceof AuthorizedString ){
           AuthorizedString as = (AuthorizedString)obj ;
           String command = as.toString() ;
           if( command.length() < 1 )return ;
           Object reply = null ;
           try{
              say( "Command(p="+as.getAuthorizedPrincipal()+") : "+command ) ;
              reply = _cellShell.objectCommand2( command ) ;
           }catch( CommandExitException cee ){
              reply = "Can't exit SystemShell" ;
           }
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

}
