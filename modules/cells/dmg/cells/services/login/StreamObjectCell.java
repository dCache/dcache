package dmg.cells.services.login ;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;

import jline.ConsoleReader;
import jline.History;
import dmg.cells.applets.login.DomainObjectFrame;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;
import dmg.util.CommandExitException;
import dmg.util.StreamEngine;


/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      StreamObjectCell
       extends    CellAdapter
       implements Runnable  {

  private StreamEngine   _engine ;
  private InputHandler _in    = null ;
  private PrintWriter    _out   = null ;
  private Object              _outLock = new Object() ;
  private ObjectOutputStream  _objOut = null ;
  private ObjectInputStream   _objIn  = null ;
  private InetAddress    _host ;
  private String         _user ;
  private Thread         _workerThread     = null ;
  private Constructor    _commandConst     = null ;
  private int            _commandConstMode = -1 ;
  private Class          _commandClass     = null ;
  private Object         _commandObject    = null ;
  private CellNucleus    _nucleus          = null ;
  private static final String HISTORY_FILE = "/var/log/.adminshell_history";
  private static final int HISTORY_SIZE = 50;
  private static final String CONTROL_C_ANSWER = "Got interrupt. Please issue \'logoff\' from within the admin cell to end this session.\n";
  //
  // args.argv[0] must contain a class with the following signature.
  //    <init>( Nucleus nucleus , Args args )     or
  //    <init>( Nucleus nucleus ) or
  //    <init>( Args args ) or
  //    <init>()
  //
  private static final Class [] [] _constSignature = {
    { java.lang.String.class , dmg.cells.nucleus.CellNucleus.class , dmg.util.Args.class } ,
    { dmg.cells.nucleus.CellNucleus.class , dmg.util.Args.class } ,
    { dmg.cells.nucleus.CellNucleus.class } ,
    { dmg.util.Args.class } ,
    {}
  } ;
  private static final Class [] [] _comSignature = {
          { java.lang.Object.class },
          { java.lang.String.class } ,
          { java.lang.String.class , java.lang.Object.class  },
          { java.lang.String.class , java.lang.String.class  }
  } ;
  private Method    []   _commandMethod = new Method[_comSignature.length] ;
  private Method         _promptMethod  = null ;
  private Method         _helloMethod   = null ;
  public StreamObjectCell( String name , StreamEngine engine , Args args )
         throws Exception       {

     super( name , args , false ) ;

     _engine = engine ;
     _nucleus = getNucleus() ;
     setCommandExceptionEnabled( true ) ;
     try{
         if( args.argc() < 1 )
           throw new
           IllegalArgumentException( "Usage : ... <commandClassName>" ) ;

         say( "StreamObjectCell "+getCellName()+"; arg0="+args.argv(0) ) ;
         prepareClass( args.argv(0) ) ;

         _in = createReader(args.getOpt("inputHandler"));

         _user   = engine.getUserName().getName() ;
         _host   = engine.getInetAddress() ;

     }catch( Exception e ){
        start() ;
        kill() ;
        throw e ;

     }
     _engine  = engine ;

     useInterpreter(false) ;

     start() ;

     _workerThread = _nucleus.newThread( this , "Worker") ;

     _workerThread.start() ;
  }
  private InputHandler createReader(String option) throws IOException {

      // use classic reader if specified
      if ("classic".equals(option)) {
          ControlBufferedReader classic = new ControlBufferedReader(_engine.getReader()) ;
          classic.onControlC(CONTROL_C_ANSWER);
          return classic;
      }

      // create Jline reader with file-based history
      ConsoleReader consoleReader = new ConsoleReader( _engine.getInputStream(), _engine.getWriter());
      History history = new History();
      history.setMaxSize(HISTORY_SIZE);
      try {
          history.load(new FileInputStream(HISTORY_FILE));
      } catch (FileNotFoundException e) {
          // ok, no history file found
      }
      consoleReader.setHistory(history);
      consoleReader.setUseHistory(true);

      // intercept Control+c
      consoleReader.addTriggeredAction( (char)3 , new ActionListener(){

          public void actionPerformed(ActionEvent e) {
             try {
                 Writer writer = (_out == null) ? new PrintWriter( _engine.getWriter() ) : _out;

                 writer.write(CONTROL_C_ANSWER);
                 writer.write( getPrompt() );
                 writer.flush();

             } catch (IOException e1) {
                 say("Cannot react on Control+c : " + e1);
             }
         }

      });

    JlineReader jlineReader = new JlineReader(consoleReader, false);


    /*
     * use history file if it exist and we can write into it
     *  or
     * do not exist, but we are allowed to create it
     */
    File historyFile = new File(HISTORY_FILE);
    if( (historyFile.exists() && historyFile.canWrite()) ||
            ( !historyFile.exists() && historyFile.getParentFile().canWrite() )) {
        jlineReader.setHistoryFile(HISTORY_FILE);
    }

    return jlineReader;
  }
private void prepareClass( String className )
          throws ClassNotFoundException , NoSuchMethodException  {

     NoSuchMethodException nsme = null ;
     _commandClass = Class.forName( className ) ;
     say( "Using class : "+_commandClass ) ;
     for( int i = 0 ; i < _constSignature.length ; i++ ){
        nsme = null ;
        Class [] x = _constSignature[i] ;
        say( "Looking for constructor : "+i ) ;
        for( int ix = 0 ; ix < x.length ; ix++ ){
            say( "   arg["+ix+"] "+x[ix] ) ;
        }
        try{
            _commandConst = _commandClass.getConstructor( _constSignature[i] ) ;
        }catch( NoSuchMethodException e ){
            esay( "Constructor not found : "+_constSignature[i] ) ;
            nsme = e ;
            continue ;
        }
        _commandConstMode = i ;
        break ;
     }
     if( nsme != null )throw nsme ;
     say( "Using constructor : "+_commandConst ) ;

     int validMethods = 0 ;
     for( int i= 0 ; i < _comSignature.length ; i++ ){
        try{
           _commandMethod[i] = _commandClass.getMethod(
                               "executeCommand" ,
                               _comSignature[i] ) ;
           validMethods ++ ;
        }catch(Exception e){
           _commandMethod[i]= null ;
           continue ;
        }
        say( "Using method ["+i+"] "+_commandMethod[i] ) ;
     }
     if( validMethods == 0 )
       throw new
       IllegalArgumentException( "no valid executeCommand found" ) ;

     try{
        _promptMethod = _commandClass.getMethod(
                           "getPrompt" ,
                           new Class[0]    ) ;
     }catch(Exception ee){
        _promptMethod = null ;
     }
     if( _promptMethod != null )
        say( "Using promptMethod : "+_promptMethod ) ;
     try{
        _helloMethod = _commandClass.getMethod(
                           "getHello" ,
                           new Class[0]    ) ;
     }catch(Exception ee){
        _helloMethod = null ;
     }
     if( _helloMethod != null )
        say( "Using helloMethod : "+_helloMethod ) ;
     return ;
  }
  private String getPrompt(){
     if( _promptMethod == null )return "" ;
     try{
        String s = (String)_promptMethod.invoke(
                               _commandObject ,
                               new Object[0] ) ;

        return s == null ? "" : s ;
     }catch(Exception ee ){
        return "" ;
     }
  }
  private String getHello(){
     if( _helloMethod == null )return null ;
     try{
        String s = (String)_helloMethod.invoke(
                               _commandObject ,
                               new Object[0] ) ;

        return s == null ? "" : s ;
     }catch(Exception ee ){
        return "" ;
     }
  }
  private void runConstructor() throws Exception {
     Args      extArgs = (Args)getArgs().clone() ;
     Object [] args    = null ;
     extArgs.shift() ;
     switch( _commandConstMode ){
        case 0 :
           args = new Object[3] ;
           args[0] = _user ;
           args[1] = getNucleus() ;
           args[2] = extArgs ;
           break ;
        case 1 :
           args = new Object[2] ;
           args[0] = getNucleus() ;
           args[1] = extArgs ;
           break ;
        case 2 :
           args = new Object[1] ;
           args[0] = getNucleus() ;
           break ;
        case 3 :
           args = new Object[1] ;
           args[0] = extArgs ;
           break ;
        case 4 :
           args = new Object[0] ;
           break ;

     }
     _commandObject = _commandConst.newInstance( args ) ;
  }
  public void run(){


     try{
        runConstructor() ;

        String hello = getHello() ;
        if( hello != null ){
           _out = new PrintWriter( _engine.getWriter() ) ;
           _out.println(hello) ;
           _out.print(getPrompt()) ;
           _out.flush() ;
        }
        String x = _in.readLine() ;
        say( "Initial read line : >"+x+"<" ) ;
        if( x.equals( "$BINARY$" ) ){
           say( "Opening Object Streams" ) ;
           _out.println(x) ;
           _out.flush() ;
           _objOut = new ObjectOutputStream( _engine.getOutputStream() ) ;
           _objIn  = new ObjectInputStream( _engine.getInputStream() ) ;

           runBinaryMode() ;
        }else{

           if( _out == null )_out = new PrintWriter( _engine.getWriter() ) ;
           runAsciiMode( x ) ;

        }

        /* To cleanly shut down the connection, we first shutdown the
         * output and then wait for an EOF on the input stream.
         */
        _engine.getSocket().shutdownOutput();
        while (_in.readLine() != null);
     }catch( Exception ee ){
        say( "Worker loop interrupted : "+ee ) ;
     } finally {
         say( "StreamObjectCell (worker) done." ) ;
         kill() ;
     }
  }
  private class BinaryExec implements Runnable {
      private DomainObjectFrame _frame ;
      private Thread _parent ;
      BinaryExec( DomainObjectFrame frame , Thread parent ){
          _frame  = frame ;
          _parent = parent ;
          _nucleus.newThread(this).start();
      }
      public void run(){
        Object    result = null ;
        boolean   done   = false ;
        say( "Frame id "+_frame.getId()+" arrived" ) ;
        try{
           if( _frame.getDestination() == null ){
              Object [] array  = new Object[1] ;
              array[0] = _frame.getPayload() ;
              if( _commandMethod[0] != null ){
                  say( "Choosing executeCommand(Object)" ) ;
                  result = _commandMethod[0].invoke( _commandObject , array ) ;

              }else if( _commandMethod[1] != null ){
                  say( "Choosing executeCommand(String)" ) ;
                  array[0] = array[0].toString() ;
                  result = _commandMethod[1].invoke( _commandObject , array ) ;

              }else
                  throw new
                  Exception( "PANIC : not found : executeCommand(String or Object)" ) ;
           }else{
              Object [] array  = new Object[2] ;
              array[0] = _frame.getDestination() ;
              array[1] = _frame.getPayload() ;
              if( _commandMethod[2] != null ){
                  say( "Choosing executeCommand(String destination, Object)" ) ;
                  result = _commandMethod[2].invoke( _commandObject , array ) ;

              }else if( _commandMethod[3] != null ){
                  say( "Choosing executeCommand(String destination, String)" ) ;
                  array[1] = array[1].toString() ;
                  result = _commandMethod[3].invoke( _commandObject , array ) ;
              }else
                  throw new
                  Exception( "PANIC : not found : "+
                             "executeCommand(String/String or Object/String)" ) ;
           }
        }catch( InvocationTargetException ite ){
           result = ite.getTargetException() ;
           done   = result instanceof CommandExitException  ;
        }catch( Exception ae ){
           result = ae ;
        }
        _frame.setPayload( result ) ;
        try{
          synchronized( _outLock ){
            _objOut.writeObject( _frame ) ;
            _objOut.flush() ;
            _objOut.reset() ;  // prevents memory leaks...
          }
        }catch( Exception ioe ){
            esay( "Problem sending result : "+ioe ) ;
        }
        if( done )_parent.interrupt() ;
      }
  }
  private void runBinaryMode() throws Exception {

     Object obj = null ;
     while( ( obj = _objIn.readObject() ) != null ){
        if( obj instanceof DomainObjectFrame ){
            new BinaryExec( (DomainObjectFrame)obj , Thread.currentThread() ) ;
        }else
             esay( "Won't accept non DomainObjectFrame : "+obj.getClass() ) ;
     }

  }
  private void runAsciiMode( String str ) throws Exception {
      String x = null ;
      Method    com = _commandMethod[1] != null ? _commandMethod[1] :
                                                  _commandMethod[0] ;
      Object [] obj = new Object[1] ;
      Object result = null ;
      boolean done = false ;
      while (!done) {
         if( str != null ){
            x = str ;
            str = null ;
         }else{
            x = _in.readLine() ;
         }
         if( x == null )
             break;

         obj[0] = x ;
         try{
            result = com.invoke( _commandObject , obj ) ;
         }catch( InvocationTargetException ite ){
            result = ite.getTargetException()  ;
            done   = result instanceof CommandExitException ;
         }catch(Exception e ){
            result = e ;
         }
         if( result != null ){
            String resultString = result.toString();
            if( ! resultString.equals("") ){
               _out.print( result.toString() ) ;
               if( resultString.charAt(resultString.length()-1)!='\n' )
                     _out.println("");
//               if( result instanceof Throwable )_out.println("");
            }
         }
         _out.print( getPrompt() ) ;
         _out.flush() ;
      }
  }

    public void cleanUp()
    {
        try {
            _engine.getSocket().close();
        } catch (IOException e) {
            esay("Failed to close socket: " + e);
        }
        if (_workerThread != null) {
            _workerThread.interrupt();
        }
    }
}
