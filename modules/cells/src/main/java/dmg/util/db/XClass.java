package dmg.util.db ;

public class XClass {
   private static Object __lock    = new Object() ;
   private static int    __counter;
   private String _name;
   public XClass( String name ){
     synchronized( __lock ){ 
        _name = name+"--"+__counter ;
        __counter ++ ;
     }
   }
   @Override
   protected void finalize() throws Throwable {
      synchronized( __lock ){ __counter -- ; 
        System.out.println( "Finalizing ("+__counter+") : " + _name ) ;
      }
   }
   public String toString(){ return _name ; }
}
