package dmg.cells.services ;
import  dmg.cells.nucleus.UOID ;
import  java.io.Serializable ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ServiceRequest implements Serializable  {

  private static final long serialVersionUID = -2129139695455383629L;

   private UOID     _uoid    = new UOID() ;
   private Object   _object;
   private String   _command;
   
   public ServiceRequest( String command , Serializable o ){
      _command = command ;
      _object  = o ;
   }
   public void setObject( Serializable o ){
      _object = o ;
   }
   public String toString(){
      if( _object instanceof String ){
        return "Service Request ("+_uoid+") : "+_object.toString() ;
      }else{
        return "Service Request ("+_uoid+") : "+_object.getClass().getName() ;
      }
   }
   public int hashCode(){ return  _uoid.hashCode() ; }
   public boolean equals( Object o ){ 	   
      return (o instanceof ServiceRequest ) && ((ServiceRequest)o)._uoid.equals( _uoid ) ;
   }
   public Serializable getObject(){ return (Serializable) _object ; }
   public String getCommmand(){ return _command ; }
  
  
  
}
