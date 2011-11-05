package dmg.cells.services.multicaster ;

public class MulticastUnregister extends MulticastEvent {

   static final long serialVersionUID = -3123454601201421771L;
   public MulticastUnregister( String eventClass , 
                               String eventName   ){
       super( eventClass, eventName ) ;
   }
}
