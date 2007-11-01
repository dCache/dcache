package diskCacheV111.util ;

import java.util.* ;
import java.io.* ;

public class SysTimer {
   private static boolean __initialized = false;
   private Timestamp   _timestamp    = new Timestamp() ;
   static {
      try{
         System.loadLibrary( "SysTimer" ) ;
         __initialized = true ;
      }catch(Error ee){
         System.err.println("SysTimer : Can't be initialized ...");
      }
   }
   public class Timestamp {
      private long _utime  = 0 ;
      private long _stime  = 0 ;
      private long _rtime  = 0 ;
      public String toString(){
//         return "utime="+_utime+";stime="+_stime+";rtime="+_rtime  ;
         if( _rtime == 0 )return "Null Time elapsed" ;
         
         float u = (float)(((float)_utime)/((float)_rtime)*100.) ;
         float s = (float)(((float)_stime)/((float)_rtime)*100.) ;
         
         return "System "+s+"% User "+u+"% (r="+_rtime+";s="+_stime+";u="+_utime+")" ;
      }
      public Object clone(){
          Timestamp x = new Timestamp() ;
          x._utime = _utime ;
          x._stime = _stime ;
          x._rtime = _rtime ;
          return x ;
      }
      public void substract( Timestamp x ){
         _utime -= x._utime ;
         _stime -= x._stime ;
         _rtime -= x._rtime ;
      }
      public long getRealTicks(){ return _rtime ; }
      public long getSystemTicks(){ return _stime ; }
      public long getUserTicks(){ return _utime ; }
   }
   public SysTimer(){ time( _timestamp ) ; }
   public synchronized native void times( Timestamp timestamp ) ;
   public synchronized native void rusage( Timestamp timestamp ) ;
   public synchronized boolean time( Timestamp timestamp ){
      if( __initialized ){
          times( timestamp ) ;
          return true ;
      }else{
          return false ;
      }
   }
   public synchronized Timestamp getRUsage(){
      Timestamp timestamp = new Timestamp() ;
      rusage( timestamp ) ;
      return timestamp ;
   }
   public synchronized Timestamp getDifference(){
       Timestamp st = new Timestamp() ;
       time( st ) ;
       Timestamp x = _timestamp ;
       _timestamp  =  (Timestamp)st.clone() ;
       st.substract( x ) ;
       return st ;
   }
}
