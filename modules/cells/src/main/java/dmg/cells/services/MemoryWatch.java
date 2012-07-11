package dmg.cells.services ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.util.* ;
import  java.io.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class MemoryWatch extends CellAdapter implements Runnable {

   private final static Logger _log =
       LoggerFactory.getLogger(MemoryWatch.class);

   private CellNucleus _nucleus = null ;
   private Args        _args    = null ;
   private long        _update  = 10 ;
   private Object      _lock    = new Object() ;
   private Thread      _queryThread = null ;
   private Runtime     _runtime     = Runtime.getRuntime() ;
   private boolean _output      = false ;
   private String  _outputFile  = null ;
   private int     _generations = 2 ;
   private int     _current     = 0 ;
   private int     _maxFileSize = 1024 * 1024 ;

   public MemoryWatch( String name , String args ) throws Exception {

      super( name , args , false ) ;
      _nucleus  = getNucleus() ;
      try{
         _args     = getArgs() ;
         String var = null ;
         //
         // update
         //
         if( ( var = _args.getOpt("update") ) != null ){
            try{ _update = Integer.parseInt(var) ;
            }catch(Exception ee ){
               _log.warn( "Update not accepted : "+var ) ;
            }
         }
         //
         // filesize
         //
         if( ( var = _args.getOpt("maxFilesize") ) != null ){
            try{ _maxFileSize = Integer.parseInt(var) ;
            }catch(Exception ee ){
               _log.warn( "New 'maxFilesize' not accepted : "+var ) ;
            }
         }
         //
         // generations
         //
         if( ( var = _args.getOpt("generations") ) != null ){
            try{ _generations = Integer.parseInt(var) ;
            }catch(Exception ee ){
               _log.warn( "New 'generations' not accepted : "+var ) ;
            }
         }
         if( ( var = _args.getOpt("output" ) ) != null ){
            _output = true ;
            if( ! var.equals("") ) {
                _outputFile = var;
            }
         }
         //
         // and  now the worker.
         //
         _queryThread = _nucleus.newThread( this , "queryThread" ) ;
         _queryThread.start() ;
      }catch(Exception eex ){
         start() ;
         kill() ;
         throw eex ;
      }
      start() ;
   }
   public void say( String str ){
      if( _output ){
         if( _outputFile != null ){
           try{
             PrintWriter pw = null ;
             String    name = _outputFile + "." +( _current % _generations ) ;
             File      f    = new File(name) ;
             if( f.exists() && ( f.length() > _maxFileSize ) ){
                _current++ ;
                name = _outputFile + "." +( _current % _generations ) ;
                pw = new PrintWriter(
                          new FileWriter( name , false ) ) ;
             }else{
                pw = new PrintWriter(
                          new FileWriter( name , true ) ) ;
             }
             try{
                pw.println(str) ;
             }catch(Exception ee){

             }finally{
                try{ pw.close() ; }catch(Exception eeee ){}
             }
           }catch(Exception xx){}
         }else{
             _log.info(str) ;
         }
      }
   }

   @SuppressWarnings(
       value="DM_GC",
       justification="Although bad practice, the GC call is part of the design of the cell"
   )
   public void run(){
      while( ! Thread.interrupted() ){
            _runtime.gc() ;
            long fm = _runtime.freeMemory() ;
            long tm = _runtime.totalMemory() ;
            say( " free "+fm+ " total "+tm+" used "+(tm-fm)+
                 " "+(new Date()).toString() ) ;
         try{
            long update ;
            synchronized(_lock){
                 update = _update * 1000 ;
            }
            Thread.sleep( update ) ;
         }catch(InterruptedException e ){
            break ;
         }
      }
      say( "Update thread finished" ) ;
   }
   public void getInfo( PrintWriter pw ){
      super.getInfo(pw);
      pw.println("Output  : "+
                 (_output?
                    ""+(_outputFile==null?"<stdout>":_outputFile):
                    "disabled" ) ) ;
      pw.println("Update  : "+_update+" seconds" ) ;
   }
   public String hh_set_generations = "<outputfileGenerations(1...10)>" ;
   public String ac_set_generations_$_1( Args args )throws CommandSyntaxException{
       int g = Integer.parseInt( args.argv(0) ) ;
       if( ( g < 1 ) || ( g > 10 ) ) {
           throw new
                   IllegalArgumentException("Generations not in range (1...10)");
       }
       _generations = g ;
       return "OutputFile generations = "+_generations ;
   }
   public String hh_set_maxFilesize = "<output filesize limit(>10k)>" ;
   public String ac_set_maxFilesize_$_1( Args args )throws CommandSyntaxException{
       int g = Integer.parseInt( args.argv(0) ) ;
       if( g < 1024 ) {
           throw new
                   IllegalArgumentException("maxFilesize not in range (>1k)");
       }
       _maxFileSize = g ;
       return "Maximum output filesize = "+_maxFileSize ;
   }
   public String hh_set_output = "off|on|<filename>" ;
   public String ac_set_output_$_1( Args args ){
       String what = args.argv(0) ;
       if( what.equals( "off" ) ){
          _output = false ;
          _outputFile = null ;
       }else if( what.equals( "on" ) ){
          _output = true ;
          _outputFile = null ;
       }else {
          _outputFile = what  ;
          _output = true ;
       }
       return "Output "+
                 (_output?
                    "set to "+(_outputFile==null?"stdout":_outputFile):
                    "disabled" );
   }
   public String hh_set_update = "<updateTime/sec>" ;
   public String ac_set_update_$_1( Args args )throws NumberFormatException {
     synchronized( _lock ) {
        _update = Integer.parseInt( args.argv(0) ) ;
     }
     return "Update time set to "+_update+" seconds" ;
   }
   public String ac_gc( Args args ){ _runtime.gc() ; return "" ; }

}
