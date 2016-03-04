// $Id: ExecAuth.java,v 1.1 2002-10-09 22:28:15 cvs Exp $

package diskCacheV111.admin ;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class ExecAuth implements Runnable {

   private BufferedReader  _input;
   private PrintWriter _output;
   private final String _execPath;

   public ExecAuth( String execpath ){
      _execPath = execpath ;
      new Thread(this).start() ;
   }
   private class Destroy implements Runnable {
      private final InputStream _error;
      private Destroy( InputStream in ){
         _error = in ;
         new Thread(this).start() ;
      }
      @Override
      public void run(){
         try{ while( _error.read() > -1 ) {
         }
         }catch(Exception ee){}
         try{ _error.close() ; }catch(IOException eee ){}
         System.out.println("Destroy done");
      }
   }
   private Process _process;
   private boolean _active;
   @Override
   public void run(){
       Runtime runtime = Runtime.getRuntime() ;
       try{
          _process = runtime.exec(_execPath) ;
          new Destroy(_process.getErrorStream());
          _input = new BufferedReader(
                     new InputStreamReader(_process.getInputStream())) ;

          _output = new PrintWriter(
                      new OutputStreamWriter(_process.getOutputStream())) ;
          _active = true ;
       }catch(Exception eee ){

       }
       try{
           _process.waitFor() ;
       }catch(InterruptedException ee ){

       }
       System.out.println("Done");
       _active = false ;
   }
   public String command( String command ) throws IOException {
      if( command == null ){
         _process.destroy() ;
         throw new
         EOFException("Done");
      }
      _output.println(command) ;
      _output.flush();
      return _input.readLine() ;
   }
   public static void main( String [] args )throws Exception {
       ExecAuth exec = new ExecAuth(args[0]) ;
       BufferedReader br =
                new BufferedReader(
                   new InputStreamReader( System.in ) ) ;

       while(true){
          System.out.println(
             exec.command( br.readLine() ) ) ;

       }
   }
}
