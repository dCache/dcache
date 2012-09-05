package dmg.cells.services.multicaster ;

public class MulticastClose extends MulticastEvent {

   private static final long serialVersionUID = 412444609092075746L;
   public MulticastClose( String eventClass , 
                          String eventName   ){
       super( eventClass, eventName ) ;
   }
}
