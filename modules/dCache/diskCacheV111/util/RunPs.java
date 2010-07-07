package diskCacheV111.util ;
import dmg.cells.nucleus.* ;
import java.io.*;
import java.util.*;


public class RunPs extends CellAdapter implements Runnable {
   private CellNucleus _nucleus = null ;
   private Thread      _worker  = null ;
   public RunPs(  String cellName , String args ){
      super( cellName , args , false ) ;
      _nucleus  = getNucleus() ;
      useInterpreter( true ) ;
      _worker =  _nucleus.newThread(this,"worker");
      _worker.start() ;
       start() ;
   }
   public void run(){
     say("Worker started");
     PrintWriter printer = null ;
     try{
       printer = new PrintWriter( new FileWriter( new File( "/tmp/RunPs.log" ) ) ) ;
     }catch( Exception ee ){
       esay("Can't open /tmp/RunPs.log");
       return ;
     }
     while(! Thread.currentThread().interrupted() ){
        try{
           Thread.currentThread().sleep(10000) ;
           try{
           /*
              String [] list = _nucleus.getCellNames() ;
              for( int i = 0 ; i < list.length ; i ++ ){
                  CellInfo info = _nucleus.getCellInfo( list[i] ) ;
                  if( info == null ){
                     printer.println( list[i] + " (defunc)\n" ) ;
                  }else{
                     printer.println( info.toString() + "\n" ) ;
                  }
              }
            */
              printer.println( new Date().toString() ) ;
              printer.flush();
           }catch( Exception xe ){
              esay("Can't write /tmp/RunPs.log");
              break ;
           }
        }catch(Exception ie ){
           say("Worker interrupted : "+ie);
           break;
        }
     }
     try{ printer.close() ; }catch(Exception iie){}
     say("Worker Done");
     kill() ;
   }


}

