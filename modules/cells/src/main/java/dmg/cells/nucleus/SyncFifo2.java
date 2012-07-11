/*
 * $Id: SyncFifo2.java,v 1.6 2007-05-24 13:51:49 tigran Exp $
 */

package dmg.cells.nucleus ;
import  java.util.ArrayList ;
 
/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
 
/**
 * deprecated: replace with java.concurrent.LinkedBlockingQueue
 */
@Deprecated
 public class SyncFifo2 {
   private ArrayList _v = new ArrayList() ;
   public synchronized void push( Object o ){
      _v.add( o ) ;
      notifyAll() ;
   } 
   public synchronized int size(){ return _v.size() ; }
   public synchronized Object pop() {
      while( true ){ 
         if( ! _v.isEmpty() ){            
            return _v.remove(0) ;
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
         if( ! _v.isEmpty() ){
            return _v.remove(0) ;
         }else{
            long rest = timeout - ( System.currentTimeMillis() - start ) ;
            if( rest <= 0L ) {
                return null;
            }
            try{ 
                wait( rest ) ; 
            }catch( InterruptedException e ){
               return null ;
            }
            
            
         }
      }
   } 
   public Object spy(){
      return _v.get(0) ;
   }

   public synchronized Object[] dump() {       
       return _v.toArray();       
   }   
   
 }
