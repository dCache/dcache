package dmg.cells.services.login ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellShell;
import dmg.util.Gate;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.util.Args;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      LoginCell
       extends    CellAdapter
       implements Runnable  {

  private final static Logger _log =
      LoggerFactory.getLogger(LoginCell.class);

  private StreamEngine   _engine ;
  private BufferedReader _in ;
  private PrintWriter    _out ;
  private InetAddress    _host ;
  private Subject         _subject ;
  private Thread         _workerThread ;
  private CellShell      _shell ;
  private String         _prompt;
  private boolean        _syncMode    = true ;
  private Gate           _readyGate   = new Gate(false) ;
  private int            _syncTimeout = 10 ;
  private int            _commandCounter;
  private String         _lastCommand    = "<init>" ;
  private Reader         _reader;

  public LoginCell( String name , StreamEngine engine , Args args )
         throws Exception {
     super( name , args , false ) ;
     _engine  = engine ;

     try{
         _reader = engine.getReader() ;
         _in   = new BufferedReader( _reader ) ;
         _out  = new PrintWriter( engine.getWriter() ) ;
         _subject = engine.getSubject();
         _host = engine.getInetAddress() ;

         _loadShells( args ) ;

     }catch( Exception e ){
        start() ;
        kill() ;
        throw e ;
     }
     useInterpreter(false) ;
     _prompt  = getCellName() ;
     _workerThread = new Thread( this ) ;

     _workerThread.start() ;

     start() ;
  }
  private final static Class<?>[][] _signature = {
     {
       String.class ,
       CellNucleus.class ,
       Args.class
     } ,
     {
       CellNucleus.class ,
     },
     {}
  } ;
  private void _loadShells( Args args ){
     Object [] [] objList = new Object[_signature.length][] ;
     for( int i= 0 ; i < objList.length ; i++ ) {
         objList[i] = new Object[_signature[i].length];
     }

     objList[0][0] = _subject ;
     objList[0][1] = getNucleus() ;
     objList[0][2] = new Args(args);
     objList[1][0] = getNucleus() ;

     Class<?>       c;
     Constructor<?> con = null ;
     Object      o;
     for( int i = 0 ; i < args.argc() ; i++ ){
        _log.info( "Trying to load shell : "+args.argv(i) ) ;
        try{
           c = Class.forName( args.argv(i) ) ;
           int j ;
           for( j = 0 ; j < _signature.length ; j++ ){
              try{
                 con = c.getConstructor( _signature[j] ) ;
                 break ;
              }catch(Exception e){

              }
           }
           if( j == _signature.length ) {
               throw new Exception("No constructor found");
           }

           o = con.newInstance( objList[j] ) ;
           addCommandListener( o ) ;
           _log.info( "Added : "+args.argv(i) ) ;
        }catch(Exception ee ){
           _log.warn( "Failed to load shell : "+args.argv(i)+" : "+ee ) ;
           if( ee instanceof InvocationTargetException ){
              _log.warn( "   -> Problem in constructor : "+
                ((InvocationTargetException)ee).getTargetException() ) ;
           }
        }

     }

  }
  @Override
  public void run(){
    if( Thread.currentThread() == _workerThread ){
        print( prompt() ) ;
        while( true ){
           try{
               if( ( _lastCommand = _in.readLine() ) == null ) {
                   break;
               }
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
              _log.info("EOF Exception in read line : "+e ) ;
              break ;
           }catch( Exception e ){
              _log.info("I/O Error in read line : "+e ) ;
              break ;
           }

        }
        _log.info( "EOS encountered" ) ;
        _readyGate.open() ;
        kill() ;

    }
  }
   @Override
   public void   cleanUp(){

     _log.info( "Clean up called" ) ;
     println("");
     _out.close();
     _readyGate.check() ;
     _log.info( "finished" ) ;

   }
  public void println( String str ){
     _out.print( str ) ;
     if( ( str.length() > 0 ) &&
         ( ! str.substring(str.length()-1).equals("\n") ) ) {
         _out.println("");
     }
     _out.flush() ;
  }
  public void print( String str ){
     _out.print( str ) ;
     _out.flush() ;
  }
   public String prompt(){
      return _prompt == null ? " .. > " : (_prompt+" > ")  ;
   }
   public int execute( String command ) throws Exception {
      if( command.equals("exit") ) {
          return 1;
      }

      println( command( command ) ) ;
      return 0 ;

   }
  //
  // the cell implemetation
  //
   public String toString(){ return Subjects.getDisplayName(_subject)+"@"+_host ; }
   @Override
   public void getInfo( PrintWriter pw ){
     pw.println( "            Generic Login Cell" ) ;
     pw.println( "         User  : "+Subjects.getDisplayName(_subject) ) ;
     pw.println( "         Host  : "+_host ) ;
     pw.println( " Last Command  : "+_lastCommand ) ;
     pw.println( " Command Count : "+_commandCounter ) ;
   }
   @Override
   public void   messageArrived( CellMessage msg ){

        Object obj = msg.getMessageObject() ;
        println("");
        println( " CellMessage From   : "+msg.getSourcePath() ) ;
        println( " CellMessage To     : "+msg.getDestinationPath() ) ;
        println( " CellMessage Class  : "+obj.getClass().getName() ) ;
        Class<?> c = obj.getClass() ;
        Method [] methods = c.getMethods() ;
        Object result;
       for (Method method : methods) {
           if (method.getDeclaringClass().equals(Object.class)) {
               continue;
           }
           if (!method.getName().startsWith("get")) {
               continue;
           }
           if (method.getParameterTypes().length > 0) {
               continue;
           }
           try {
               result = method.invoke(obj);
               println("    " + method.getName() + " -> " + result.toString());
           } catch (IllegalAccessException | InvocationTargetException e) {
               println("    " + method.getName() + " -> (???)");
           }

       }
        print( prompt() );

   }
}
