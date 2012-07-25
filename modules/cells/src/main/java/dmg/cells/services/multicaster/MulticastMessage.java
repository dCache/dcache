package dmg.cells.services.multicaster ;

public class MulticastMessage extends MulticastEvent {
   static final long serialVersionUID = 8630314096425808971L;
   private Object _message;
   public MulticastMessage( String eventClass , 
                            String eventName  ,
                            Object message      ){
       super( eventClass, eventName ) ;
       _message = message ;
   }
   public MulticastMessage( String eventClass , 
                            String eventName   ){
       super( eventClass, eventName ) ;
   }
   public void setMessage( Object message ){ _message = message ; }
   public Object getMessage(){return _message ; }
   public String toString(){
     return super.toString()+
        ";message="+(_message==null?"<none>":_message.toString()) ;
   }
}
