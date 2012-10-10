package dmg.cells.services.multicaster ;

import java.io.Serializable;

public class MulticastMessage extends MulticastEvent {
   private static final long serialVersionUID = 8630314096425808971L;
   private Object _message;
   public MulticastMessage( String eventClass , 
                            String eventName  ,
                            Serializable message      ){
       super( eventClass, eventName ) ;
       _message = message ;
   }
   public MulticastMessage( String eventClass , 
                            String eventName   ){
       super( eventClass, eventName ) ;
   }
   public void setMessage( Serializable message ){ _message = message ; }
   public Serializable getMessage(){return (Serializable) _message ; }
   public String toString(){
     return super.toString()+
        ";message="+(_message==null?"<none>":_message.toString()) ;
   }
}
