package dmg.util.db ;

public class XClass {
   private static final Object __lock    = new Object() ;
   private static int    __counter = 0 ;
   private String _name = null ;
   public XClass( String name ){
     synchronized( __lock ){ 
        _name = name+"--"+__counter ;
        __counter ++ ;
     }
   }
   protected void finalize() throws Throwable {
      synchronized( __lock ){ __counter -- ; 
        System.out.println( "Finalizing ("+__counter+") : " + _name ) ;
      }
   }
   public String toString(){ return _name ; }
}
