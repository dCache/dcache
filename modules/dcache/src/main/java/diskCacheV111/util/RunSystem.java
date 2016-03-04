// $Id: RunSystem.java,v 1.7 2007-09-04 15:55:38 tigran Exp $

package diskCacheV111.util ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public class RunSystem implements Runnable {
    private static final Logger _log = LoggerFactory.getLogger(RunSystem.class);
    private static final Runtime __runtime = Runtime.getRuntime() ;
    private final String[] _exec ;
    private final int    _maxLines ;
    private final long   _timeout ;
    private final Thread _readErrorThread ;
    private final Thread _readOutputThread ;
    private final Thread _processThread ;
    private final int     _id            = nextId() ;
    private Process _process ;
    private int     _stoppedReader;
    private boolean _processDone;
    private boolean _linesExceeded;
    private boolean _interrupted;
    private BufferedReader _stdout;
    private BufferedReader _stderr;
    private final PrintWriter _errorPrintWriter   ;
    private final PrintWriter _outputPrintWriter  ;
    private final StringWriter _errorStringWriter ;
    private final StringWriter _outputStringWriter;

    private static int __counter = 100 ;
    private static synchronized int nextId(){ return __counter++ ; }

    public RunSystem(int maxLines, long timeout, String ... exec)
    {
        _exec     = exec ;
        _maxLines = maxLines ;
        _timeout  = timeout ;
        _readErrorThread    = new Thread( this , "error" ) ;
        _readOutputThread   = new Thread( this , "output" ) ;
        _processThread      = new Thread( this , "process" ) ;

        _outputStringWriter = new StringWriter() ;
        _errorStringWriter  = new StringWriter() ;
        _outputPrintWriter  = new PrintWriter( _outputStringWriter ) ;
        _errorPrintWriter   = new PrintWriter( _errorStringWriter ) ;

    }

    private void say(String str) {
        if (_log.isDebugEnabled()) {
            _log.debug("[" + _id + "] " + str);
        }
    }

    private void interruptReaders(){
       _readOutputThread.interrupt() ;
       _readErrorThread.interrupt() ;
    }
    public void go() throws IOException {
       _process = __runtime.exec( _exec ) ;
       _stdout = new BufferedReader(
                    new InputStreamReader( _process.getInputStream() ) );
       _stderr  = new BufferedReader(
                    new InputStreamReader( _process.getErrorStream() ) );

       /*
        * we do not need stdin of the process. To avoid file descriptor leaking close it.
        */
       _process.getOutputStream().close();
       synchronized( this ){
          _readErrorThread.start() ;
          _readOutputThread.start() ;
          _processThread.start() ;

          try{
             long end = System.currentTimeMillis() + _timeout ;
             while( ( _stoppedReader < 2 ) || ( ! _processDone )  ){
                long rest = end - System.currentTimeMillis() ;
                if( rest <= 0 ) {
                    break;
                }
                wait( rest ) ;
                say( "Master : Wait returned : "+statusPrintout() ) ;
             }
          }catch(InterruptedException ie ){
//             say("Master : interrupted (interrupting the others)
//             interruptAll() ;
             if( ! _processDone ){
                say( "Master : Destroying process" ) ;
                _process.destroy() ;
             }
          }
          say( "Master : Wait stopped : "+statusPrintout() ) ;
          //
          // now wait some time for a regular shutdown condition
          // ( we have to do it 2 times because, on a regular
          //   job finish, the interruptAll had not been called.
          //
          for( int l = 0 ; l < 20000 ; l++ ){
             say( "Master : Wait loop "+l+" started" ) ;
             try{
                long end = System.currentTimeMillis() + 5 * 1000 ;
                while( ( _stoppedReader < 2 ) || ( ! _processDone )  ){
                   long rest = end - System.currentTimeMillis() ;
                   if( rest <= 0 ) {
                       break;
                   }
                   wait( rest ) ;
                   say( "Master : Wait 2 returned : "+statusPrintout() ) ;
                }
             }catch(InterruptedException ie ){
                say("Master : wait2 interrupted" ) ;
             }
             say( "Master : Wait2 loop : "+l+" : "+statusPrintout() ) ;
             if(  ( _stoppedReader > 1 ) && _processDone ) {
                 break;
             }
             if( ! _processDone ){
                say( "Master : Wait 2 loop : Destroying process" ) ;
                _process.destroy() ;
             }
             if( ( l > 2 ) && ( _stoppedReader < 2 ) ){
                 say( "Master : Wait 2 loop : Interrupting readers" ) ;
                 interruptReaders() ;
             }
          }
       }
    }
    private String statusPrintout(){
        return    ";interrupt="+_interrupted+
                  ";done="+_processDone+
                  ";count="+_stoppedReader ;

    }
    @Override
    public void run(){
       if( Thread.currentThread() == _readErrorThread ){
           runReader( _stderr , _errorPrintWriter  ) ;
       }else if( Thread.currentThread() == _readOutputThread ){
           runReader( _stdout , _outputPrintWriter ) ;
       }else if( Thread.currentThread() == _processThread ){
           runProcess() ;
       }

    }
    public String getErrorString(){
       return _errorStringWriter.getBuffer().toString() ;
    }
    public String getOutputString(){
       return _outputStringWriter.getBuffer().toString() ;
    }
    public int getExitValue() throws IllegalThreadStateException {
       return _process.exitValue() ;
    }
    private void runProcess(){

       try{
           say( "Process : waitFor called" ) ;
           int rr = _process.waitFor() ;
           say( "Process : waitFor returned ="+rr+"; waiting for sync" ) ;
       }catch( InterruptedException ie ){
           synchronized(this){
              _interrupted = true ;
              say( "Process : waitFor was interrupted " ) ;
           }
       }finally{
           synchronized(this){
              _processDone = true ;
              say("Process : done" ) ;
              notifyAll() ;
           }
       }
    }
    private void runReader( BufferedReader in , PrintWriter out ){
        int lines = 0 ;
        String line;
        try{
           say( "Reader started" ) ;
           while( ( ! Thread.interrupted() ) &&
                  (  ( line = in.readLine() ) != null     )    ){

                if( (lines++) < _maxLines ) {
                    out.println(line);
                }

           }
        }catch( InterruptedIOException  iioe ){
           say( "Reader interruptedIoException" ) ;
        }catch( Exception ioe ){
           say( "Reader Exception : "+ioe ) ;
        }finally{
           say( "Reader closing streams" ) ;
           try{ in.close() ; }catch(IOException e){}
           out.close() ;
           synchronized(this){
              say("Reader finished" ) ;
              if( lines >= _maxLines ) {
                  say("Lines (" + lines + ") have been truncated after " + _maxLines);
              }
              _stoppedReader++ ;
              notifyAll() ;
           }
        }

    }
    public static void main( String [] args ) throws Exception {
        if( args.length < 3 ){
            System.err.println( "Usage : ... <systemClass> <maxLines> <timeout>" ) ;
            System.exit(4) ;
        }
        long timeout = Long.parseLong( args[2] ) * 1000 ;
        int  maxLines = Integer.parseInt( args[1] ) ;

        RunSystem run = new RunSystem(maxLines, timeout, args[0] ) ;
        run.go() ;

        int rc = run.getExitValue() ;
        System.out.println("Exit Value : "+rc ) ;
        System.out.println("--------------- Output ------------" ) ;
        System.out.println( run.getOutputString() ) ;
        System.out.println("--------------- Error ------------" ) ;
        System.out.println( run.getErrorString() ) ;
        System.exit(rc) ;
    }

}
