package dmg.cells.services.multicaster ;

import java.io.Serializable;

public class MulticastOpen extends MulticastEvent {

   private static final long serialVersionUID = -353330052073856189L;
   private Object  _detail;
   private boolean _overwrite = true ;
   private Object  _state;
   public MulticastOpen( String eventClass , 
                         String eventName ,
                         Serializable serverDetail ){
       super( eventClass, eventName ) ;
       _detail = serverDetail ;                     
   }
   public void setServerState( Serializable state ){ _state = state ; }
   public Serializable getServerState(){ return (Serializable) _state ; }
   public void setOverwrite( boolean overwrite ){ _overwrite = overwrite ; }
   public boolean isOverwrite(){ return _overwrite ; }
   public Serializable getServerDetail(){ return (Serializable) _detail ; }
}
