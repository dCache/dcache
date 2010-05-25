package dmg.util.db ;

import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;
import   dmg.protocols.ssh.* ;

import java.util.* ;
import java.io.* ;
import java.net.* ;


/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      DbLoginCell
       extends    CellAdapter
       implements Runnable  {

  private StreamEngine   _engine ;
  private BufferedReader _in ;
  private PrintWriter    _out ;
  private InetAddress    _host ;
  private String         _user ;
  private Thread         _workerThread ;
  private CellShell      _shell ;
  private String         _destination = null ;
  private boolean        _syncMode    = true ;
  private Gate           _readyGate   = new Gate(false) ;
  private int            _syncTimeout = 10 ;
  private int            _commandCounter = 0 ;
  private String         _lastCommand    = "<init>" ;
  private Reader         _reader = null ;

  private DbResourceHandler _handler   = null ;
  private String            _container = "/home/patrick/cells/dmg/util/db/container" ;
  private DbResourceHandle  _handle    = null ;
  private XClass            _xobject   = null ;

  public DbLoginCell( String name , StreamEngine engine ){
     super( name ) ;
     _engine  = engine ;

     _reader = engine.getReader() ;
     _in   = new BufferedReader( _reader ) ;
     _out  = new PrintWriter( engine.getWriter() ) ;
     _user = Subjects.getUserName(engine.getSubject());     
     _host = engine.getInetAddress() ;

     _destination  = getCellName() ;
     _workerThread = new Thread( this ) ;

     _workerThread.start() ;
     setPrintoutLevel( 11 ) ;
     useInterpreter(false) ;
     //
     //
     // create the database if not yet done
     //
     Map<String,Object> dict = getDomainContext() ;
     _handler = (DbResourceHandler)dict.get( "database" ) ;
     if( _handler == null ){
        _container = (String) dict.get( "databaseName" ) ;
        if( _container == null ){
           kill() ;
           throw new IllegalArgumentException( "databaseName not defined" ) ;
        }
        _handler = new DbResourceHandler( new File( _container ) , false ) ;
        dict.put( "database" , _handler ) ;
        say( "Database handler created on : "+_container ) ;
     }
  }
  public void run(){
    if( Thread.currentThread() == _workerThread ){
        print( prompt() ) ;
        while( true ){
           try{
               if( ( _lastCommand = _in.readLine() ) == null )break ;
               _commandCounter++ ;
               if( execute( _lastCommand ) > 0 ){
                  //
                  // we need to close the socket AND
                  // have to go back to readLine to
                  // finish the ssh protocol gracefully.
                  //
                  try{ _out.close() ; }catch(Exception ee){}
               }else{
                  print( prompt() ) ;
               }
           }catch( IOException e ){
              esay("EOF Exception in read line : "+e ) ;
              break ;
           }catch( Exception e ){
              esay("I/O Error in read line : "+e ) ;
              break ;
           }

        }
        say( "EOS encountered" ) ;
        _readyGate.open() ;
        kill() ;

    }
  }
  public String ac_setp_$_2( Args args )throws CommandException{
     if( _handle == null )throw new CommandException("No Handle assigned" ) ;
     try{
        _handle.setAttribute( args.argv(0) , args.argv(1) ) ;
     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }
     return "Done on "+_handle.getName() ;
  }
  public String ac_showp_$_1( Args args )throws CommandException{
     if( _handle == null )throw new CommandException("No Handle assigned" ) ;
     String par = null ;
     try{
        par = (String)_handle.getAttribute( args.argv(0) ) ;
     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }
     return args.argv(0) +" = "+par ;
  }
  public String ac_show_handle( Args args )throws CommandException{
      if( _handle == null )throw new CommandException("No Handle assigned" ) ;
      return "Current Handle points to "+_handle.toString() ;
  }
  public String ac_show_thread( Args args )throws CommandException{
      return "Current Thread is "+Thread.currentThread() ;
  }
  public String ac_show_cache( Args args )throws CommandException{
      return "Cache size is "+_handler.getCacheSize() ;
  }
  public String hh_mxcreate  = " <interval> <numberOfIntevals>" ;
  public String ac_mxcreate_$_2( Args args ) throws CommandException {

     int inter = Integer.parseInt( args.argv(0) ) ;
     int count = Integer.parseInt( args.argv(1) ) ;

     _handle = null ;
     long start , now ;
     try{
        int positionName =  10000 ;
        int position     =  0 ;
        for( int j = 0 ; j < count ; j++ ){
           start = System.currentTimeMillis() ;
           for(  int i = 0 ; i < inter ; i++ , position++ , positionName++ ){
               _handler.createResource( "X"+positionName ) ;
           }
           now = System.currentTimeMillis() ;
           System.out.println( ""+( position-inter/2 ) +
                               "  "+( (double)(now-start) / (double)inter ) ) ;
        }
        return "Done" ;
     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }

  }
  public String hh_mcreate  = " <startNumber>  <last-1 Number>" ;
  public String ac_mcreate_$_2( Args args ) throws CommandException {
     int from = Integer.parseInt( args.argv(0) ) ;
     int to   = Integer.parseInt( args.argv(1) ) ;
     _handle = null ;
     try{
        long start = System.currentTimeMillis() ;
        for( int i = from ; i < to ; i++ ){
            _handler.createResource( "X"+i ) ;
        }
        long now = System.currentTimeMillis() ;
        return "Needed "+(now-start)+" millis to create "+
                            (to-from)+" Entries" ;
     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }

  }
  public String hh_create  = " <resourceName>" ;
  public String ac_create_$_1( Args args ) throws CommandException {
     try{
        _handle = _handler.createResource( args.argv(0) ) ;
     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }
     return "Current Handle points to "+_handle.getName() ;
  }
  public String hh_get = " <resourceName>" ;
  public String ac_get_$_1( Args args ) throws CommandException {
     try{
      _handle = _handler.getResourceByName( args.argv(0) ) ;

     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }
     return "Current Handle points to "+_handle.getName() ;
  }
  public String hh_open = " read|write " ;
  public String ac_open_$_1( Args args ) throws CommandException {
     if( _handle == null )throw new CommandException("No Handle assigned" ) ;
     try{
        int mode = args.argv(0).equals("read") ?
                   DbGLock.READ_LOCK :
                   DbGLock.WRITE_LOCK  ;

        _handle.open( mode ) ;

     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }
     return _handle.getName()+" opened" ;
  }
  public String ac_close( Args args ) throws CommandException {
     if( _handle == null )throw new CommandException("No Handle assigned" ) ;
     try{
        _handle.close() ;

     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }
     return _handle.getName()+" closed" ;
  }
  public String ac_remove( Args args ) throws CommandException {
     if( _handle == null )throw new CommandException("No Handle assigned" ) ;
     try{
        _handle.remove() ;

     }catch( Exception dbe ){
        throw new CommandException( dbe.toString()) ;
     }
     return _handle.getName()+" removed" ;
  }
  public String ac_release( Args args ) throws CommandException {
     _handle = null ;
     return "Current Handle released" ;
  }
  public String ac_x( Args args ){
     String name = new Date().toString() ;
     _xobject = new XClass( name ) ;
     String x = _xobject.toString() ;
     _xobject = null ;
     return "Created name : "+x ;
  }
  //
  //    this and that
  //
   public void   cleanUp(){

     say( "Clean up called" ) ;
     println("");
     try{ _out.close() ; }catch(Exception ee){}
     _readyGate.check() ;
     say( "finished" ) ;

   }
  public void println( String str ){
     _out.print( str ) ;
     if( ( str.length() > 0 ) &&
         ( str.charAt(str.length()-1) != '\n' ) )_out.print("\n") ;
     _out.flush() ;
  }
  public void print( String str ){
     _out.print( str ) ;
     _out.flush() ;
  }
   public String prompt(){
      return _destination == null ? " .. > " : (_destination+" > ")  ;
   }
   public int execute( String command ) throws Exception {
      if( command.equals("") )return 0 ;

         try{
             println( command( command ) ) ;
             return 0 ;
         }catch( CommandExitException cee ){
             return 1 ;
         }

   }
   private void printObject( Object obj ){
      if( obj == null ){
         println( "Received 'null' Object" ) ;
         return ;
      }
      String output = null ;
      if( obj instanceof Object [] ){
         Object [] ar = (Object []) obj ;
         for( int i = 0 ; i < ar.length ; i++ ){
            if( ar[i] == null )continue ;

            print( output = ar[i].toString() ) ;
            if(  ( output.length() > 0 ) &&
                 ( output.charAt(output.length()-1) != '\n' )

               )print("\n") ;
         }
      }else{
         print( output =  obj.toString() ) ;
         if( ( output.length() > 0 ) &&
             ( output.charAt(output.length()-1) != '\n' ) )print("\n") ;
      }

   }
  //
  // the cell implemetation
  //
   public String toString(){ return _user+"@"+_host ; }
   public void getInfo( PrintWriter pw ){
     pw.println( "            Stream LoginCell" ) ;
     pw.println( "         User  : "+_user ) ;
     pw.println( "         Host  : "+_host ) ;
     pw.println( " Last Command  : "+_lastCommand ) ;
     pw.println( " Command Count : "+_commandCounter ) ;
   }
   public void   messageArrived( CellMessage msg ){

        Object obj = msg.getMessageObject() ;
        println("");
        println( " CellMessage From   : "+msg.getSourceAddress() ) ;
        println( " CellMessage To     : "+msg.getDestinationAddress() ) ;
        println( " CellMessage Object : "+obj.getClass().getName() ) ;
        printObject( obj ) ;

   }
   ///////////////////////////////////////////////////////////////////////////
   //                                                                       //
   // the interpreter stuff                                                 //
   //                                                                       //
   public String ac_set_timeout_$_1( Args args ) throws Exception {
      _syncTimeout = new Integer( args.argv(0) ).intValue() ;
      return "" ;
   }
   public String ac_exit( Args args ) throws CommandExitException {
      throw new CommandExitException( "" , 0 ) ;
   }



}
