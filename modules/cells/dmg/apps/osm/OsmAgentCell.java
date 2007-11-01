package dmg.apps.osm ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.util.* ;
import  java.io.* ;

public class OsmAgentCell 
       extends CellAdapter 
       implements Runnable, Logable  {
       
   private CellNucleus _nucleus = null ;
   private Args        _args    = null ;
   private LibraryInfo _info    = null ;
   private String      _libraryName = null ;
   private OsmAgent    _agent   = null ;
   private Thread      _queryThread = null ;
   private Object      _lock        = new Object() ;
   private long        _update      = 30 ;
   private boolean     _isDummy     = false ;
   private long        _lastUpdated = 0L ;
   private Thread      _watchDogThread = null ;
   
   public OsmAgentCell( String name , String args ) throws Exception {
     super( name , args , false ) ;
     _nucleus = getNucleus() ;
     _args    = getArgs() ;
     try{
        _libraryName = _args.argc() > 0 ? _args.argv(0) : name ;
        say( "Using library : "+_libraryName ) ;
        String var = null ;
        if( ( var = _args.getOpt("osmd" ) ) != null ){
           String osmd = var ;
           if( ( var = _args.getOpt("osmq") ) == null )
              throw new 
              IllegalArgumentException( "Need 'osmq' as well" ) ;
           _agent   = new OsmAgent( osmd , var ) ;
           _isDummy = true ;
        }else{
           say( "Is real mode" ) ;
           _agent   = new OsmAgent( _libraryName , this ) ;
           say( "OsmAgent created" ) ;
           _isDummy = false ;
           _info    = _agent.getLibraryInfo() ;
           say( "getLibrary successful" ) ;
        }  
        //
        // do we need a customized update interval.
        //
        String up = _args.getOpt("update") ;
        try{
           if( up != null ){
              _update = Integer.parseInt(up) ;
           }
        }catch(Exception ee ){
           esay( "Update not accepted : "+up ) ;
        }
        say( "Update = "+_update ) ;
        //
        // and  now the worker.
        //
        if( ! _isDummy  ){
           say( "Starting queryThread" ) ;
           _queryThread = _nucleus.newThread( this , "queryThread" ) ;
           say( "QueuryThread created" ) ;
           _queryThread.start() ;
           say( "QueuryThread started" ) ;
           ( _watchDogThread = _nucleus.newThread(this,"watchDog") ).start() ;
        }    
     }catch(Exception e ){
        esay( "Exception in <init> : "+e ) ; 
        esay(e) ;
        start() ;
        kill() ;
        throw e ;
     }     
     start() ;
     say( "<init> : und nun flieg" ) ;
     useInterpreter(true) ;
   }
   public synchronized void say( String str ){
      pin(str) ; super.say(str)  ;
   }
   public synchronized void esay( String str ){
      pin(str) ; super.esay(str) ;
   }
   public void log(String str){  say( str ) ;}
   public void elog(String str){ esay( str ) ; }
   public void plog(String str){ esay( "PANIC : "+str ) ; }
   public String ac_set_update_$_1( Args args )throws Exception {
     synchronized( _lock ) {
        _update = Integer.parseInt( args.argv(0) ) ;
     }
     return "Update time set to "+_update+" seconds" ;
   }
   public Object ac_get_info( Args args )throws Exception {
      if( _info == null )
        throw new
        IllegalArgumentException( "Not yet initialized(try again" ) ;
      synchronized( _lock ){
         LibraryInfo info = _info ;
         info.setUpdate((int)_update) ;
         return info ;
      } 
   }
   public String ac_get_update( Args args ) {
      return ""+_update ;
   }
   public String ac_set_histogram_binwidth_$_1( Args args )throws Exception {
     int binWidth = Integer.parseInt( args.argv(0) ) ;
     _agent.setQueueHistogramBinWidth(binWidth) ;
     return "Queue Histogram Binwidth set to "+binWidth+" seconds" ;
   }
   public String ac_get_histogram_binwidth( Args args ){
      return ""+_agent.getQueueHistogramBinWidth() ;
   }
   public void run(){
     Thread ct = Thread.currentThread() ;
     if( ct == _queryThread ){
        runGetInfo() ;
     }else if( ct == _watchDogThread ){
        runWatchDog() ;
     }
   }
   private void runWatchDog(){
      while( ! Thread.interrupted() ){
         long now = System.currentTimeMillis() ;
         if( ( _lastUpdated != 0L ) &&
             ( ( now - _lastUpdated ) > ( 3000 * _update ) ) ){
             
             // kill() ;
             //
             // we should use kill here, but there is a chance,
             // that the systems starts to loop (IRIX only)
             //
             System.exit(4);
             break ;   
         }
         try{
            Thread.sleep(60000) ;
         }catch(InterruptedException ee ){
            break ;
         }
      
      }
   }
   private void runGetInfo(){
      LibraryInfo info = null ;
      while( ! Thread.interrupted() ){
         _lastUpdated = System.currentTimeMillis() ;
         try{
            say( "run : getLibraryInfo" ) ;
            info = _agent.getLibraryInfo() ;
            say( "run : getLibraryInfo done" ) ;
         }catch(Exception ee ){
            esay( "Problem obtaining lib info : "+ee ) ;
            continue ;
         }     
         try{
            long update ;
            synchronized(_lock){ 
                 update = _update * 1000 ; 
                 _info  = info ;
            } 
            long start = System.currentTimeMillis() ;
            long rest  = update ;
            while( rest > 0 ){ 
               say( "Going to sleep : "+rest ) ;
               Thread.sleep( rest ) ;
               rest = update - ( System.currentTimeMillis() - start ) ;             
            }
            say( "Woking up" ) ;
         }catch(Exception e ){
            esay("run : interrupted" ) ;
            break ;
         }
      }
      say( "Update thread finished" ) ;
   }
   public void getInfo( PrintWriter pw ){
      super.getInfo(pw) ;
      pw.println( "Watching library : "+_libraryName ) ;
      pw.println( "Update Time      : "+_update+" seconds" ) ;
   }
   public void cleanUp(){
   
   }
}
