package dmg.cells.nucleus ;
import  java.io.Serializable ;
import  java.util.Date ;

/**
  *  uoid is the 'Unique Message Identifier'.
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
  *   WARNING : This Class is designed to be imutual.
  *             All other class rely on that fact and
  *             a lot of things may fail at runtime
  *             if this design item is changed.
  */
public class UOID implements Serializable , Cloneable {
   static final long serialVersionUID = -5940693996555861085L;
   private static long __counter = 100 ;
   private long   _counter ;
   private long   _time ;
   /**
     *   The constructor creates an instance of an uoid
     *   which is assumed to be different from all other
     *   uoid's created before that time and therafter.
     *   This behaviour is garantued for all uoids created
     *   inside of one virtual machine and very likely for
     *   all others.
     */
   public UOID()  {
       _time    = new Date().getTime() ;
       _counter = __getNextNumber() ;
       return ;
   }
   public Object clone(){ 
     try{
         return super.clone() ;
     }catch( CloneNotSupportedException cnse ){
         return null ;
     }
   }
   /*
   UOID getClone(){ 
      try { 
         return (UOID)this.clone() ; 
      }catch( CloneNotSupportedException cnse ){
         return null ;
      }
   }
   */
   /**
     *  creates a hashcode which is more optimal then the object hashCode.
     */
   public int hashCode(){  
//      System.out.println( " hashCode called " ) ;
      return (int) ( _counter & 0xffffffff ) ;
   } 
   /**
     *  compares two uoids and overwrites Object.equals.
     */
   public boolean equals( Object x ){
//      System.out.println( " equals called " ) ;
	   if( !( x instanceof UOID) ) return false;
      UOID u = (UOID)x ;
      return ( u._counter == _counter ) && ( u._time == _time ) ;
   }
   public String toString(){
     return "<"+_time+":"+_counter+">" ;
   }
   private static synchronized long __getNextNumber(){
      return __counter++ ;
   }
   public static void main( String  [] args ){
     if( args.length == 0 ){
         UOID a = new UOID() ;
         System.out.println(" UOID : "+a ) ;
     }else{
         Date date = new Date( Long.parseLong(args[0]));
         System.out.println( date.toString() );
     }

   }
}
