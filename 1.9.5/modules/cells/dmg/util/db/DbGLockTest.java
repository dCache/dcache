package dmg.util.db ;

import java.util.* ;

public class DbGLockTest {
   private DbGLock _lock = null ;
   private class LockThread implements Runnable {
      private Thread _thread = null ;
      private int    _flags  = 0 ;
      private LockThread( int flags ){
         _flags  = flags ; 
         _thread = new Thread( this ) ;
         _thread.start() ;
      }
      public void run(){
         try{
           if( _flags == 1 ){
               System.out.println(""+_thread+" : trying to get READ lock" ) ;
               _lock.open( DbGLock.READ_LOCK ) ;
               System.out.println(""+_thread+" : could get READ lock" ) ;
               try{_thread.sleep(2000) ; }catch( Exception e ){}
           }else if( _flags == 2 ){
               System.out.println(""+_thread+" : trying to get WRITE lock" ) ;
               _lock.open( DbGLock.WRITE_LOCK ) ;
               System.out.println(""+_thread+" : could get WRITE lock" ) ;
               try{_thread.sleep(2000) ; }catch( Exception e ){}
           }else if ( _flags == 3 ){
               System.out.println(""+_thread+" : trying to get READ* lock" ) ;
               _lock.open( DbGLock.READ_LOCK ) ;
               System.out.println(""+_thread+" : could get READ* lock" ) ;
               try{_thread.sleep(1000) ; }catch( Exception e ){}
               System.out.println(""+_thread+" : trying to get WRITE* lock" ) ;
               _lock.open( DbGLock.WRITE_LOCK ) ;
               System.out.println(""+_thread+" : could get WRITE* lock" ) ;
               try{_thread.sleep(1000) ; }catch( Exception e ){}
               System.out.println(""+_thread+" : releasing lock" ) ;
               _lock.close() ;
           }
            System.out.println(""+_thread+" : releasing lock" ) ;
            _lock.close() ;
            System.out.println(""+_thread+" : Done" ) ;
         }catch( Exception ee ){
            System.out.println(""+_thread+" : "+ee ) ;
         }
      }
   
   }

   public DbGLockTest() throws Exception {
       _lock = new DbGLock() ;
  
       for( int i = 0 ; i < 2000  ; i++ ){
          int m = i % 4 ;
          if( m == 0 ){
             new LockThread( 2 ) ;
          }else if( m == 1 ){
             new LockThread( 3 ) ;
          }else{
             new LockThread( 1 ) ;
          }
//          try{ Thread.sleep(1000) ; }
//          catch( Exception e){}
          System.err.println( _lock.toString() ) ;
       }       
 
   }

   public static void main( String [] args )throws Exception {
   
      new DbGLockTest() ;
   }
}
