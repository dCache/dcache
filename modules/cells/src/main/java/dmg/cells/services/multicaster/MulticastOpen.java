package dmg.cells.services.multicaster ;

public class MulticastOpen extends MulticastEvent {

   static final long serialVersionUID = -353330052073856189L;
   private Object  _detail;
   private boolean _overwrite = true ;
   private Object  _state;
   public MulticastOpen( String eventClass , 
                         String eventName ,
                         Object serverDetail ){
       super( eventClass, eventName ) ;
       _detail = serverDetail ;                     
   }
   public void setServerState( Object state ){ _state = state ; }
   public Object getServerState(){ return _state ; }
   public void setOverwrite( boolean overwrite ){ _overwrite = overwrite ; }
   public boolean isOverwrite(){ return _overwrite ; }
   public Object getServerDetail(){ return _detail ; }
}
