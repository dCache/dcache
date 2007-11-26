package dmg.util ;

public interface Logable {
   /**
    *   Should log information only
    */
   public void log(  String message ) ;
   /**
    *   Should log errors
    */
   public void elog( String message ) ;
   /**
    *   Should log unrecoverable problems
    */
   public void plog( String message ) ;

}
