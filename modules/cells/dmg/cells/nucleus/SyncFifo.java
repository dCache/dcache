package dmg.cells.nucleus ;
import  java.util.Vector ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */

 public class SyncFifo extends Vector {

   public synchronized void push( Object o ){
      addElement( o ) ;
      notifyAll() ;
   }
   public synchronized Object pop() {
      while( true ){
         if( ! isEmpty() ){
            Object tmp = firstElement() ;
            removeElementAt(0) ;
            return tmp ;
         }else{
            try{
               wait() ;
            }catch( InterruptedException e ){ return null ;}
         }
      }
   }
   public synchronized Object pop( long timeout ) {
      long start = System.currentTimeMillis() ;
      while( true ){
         if( ! isEmpty() ){
            Object tmp = firstElement() ;
            removeElementAt(0) ;
            return tmp ;
         }else{
            long rest = timeout - ( System.currentTimeMillis() - start ) ;
            if( rest <= 0L )return null ;
            try{
                wait( rest ) ;
            }catch( InterruptedException e ){
               return null ;
            }


         }
      }
   }
   public Object spy(){
      return firstElement() ;
   }
//   protected void finalize() throws Throwable {
//     super.finalize() ;
////     System.out.println( " FIFO finalizer called " ) ;
//   }
 }
