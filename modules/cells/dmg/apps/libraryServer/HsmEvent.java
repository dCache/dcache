package dmg.apps.libraryServer ;
import java.awt.* ;
import java.awt.event.* ;
import java.util.*;

public class HsmEvent extends ActionEvent {
   static private class ErrorEntry {
       private int     _error = 0 ;
       private String _command = "" ;
       private String _detail  = null ;
       private ErrorEntry( int error , String command ){
           this(error,command,null);
       }
       private ErrorEntry( int error , String command , String detail ){
          _error   = error ;
          _command = command ;
          _detail  = detail ;
       }
       private int getId(){ return _error ; }
       private String getCommand(){ return _command ; }
       private String getCommandString(String detail ){
       
          String d = detail!=null?detail:_detail!=null?_detail:null ;
          
          return _error+" "+_command+(d==null?"":(" \""+d+"\""))  ; 
       }
       public String toString(){ return _command+"("+_error+")" ;}
       private String getDetail(){ return _detail ; }
   }
   private static Hashtable __errorHash = new Hashtable() ;
   private static final ErrorEntry [] _errors = {
      new HsmEvent.ErrorEntry( 111 , "try-again" ) ,
      new HsmEvent.ErrorEntry( 112 , "internal-system-error" ) ,
      new HsmEvent.ErrorEntry( 113 , "timeout" ) ,
      new HsmEvent.ErrorEntry( 124 , "command-not-found" ) ,
      new HsmEvent.ErrorEntry( 125 , "illegal-aguments" ) ,
      new HsmEvent.ErrorEntry( 200 , "mount" ) ,
      new HsmEvent.ErrorEntry( 201 , "mount-ok") ,
      new HsmEvent.ErrorEntry( 211 , "mount-failed" ) ,
      new HsmEvent.ErrorEntry( 212 , "mount-failed" , "Drive Broken") ,
      new HsmEvent.ErrorEntry( 213 , "mount-failed" , "Tape Not Found") ,
      new HsmEvent.ErrorEntry( 300 , "dismount" ) ,
      new HsmEvent.ErrorEntry( 301 , "dismount-ok") ,
      new HsmEvent.ErrorEntry( 311 , "dismount-failed" ) ,
      new HsmEvent.ErrorEntry( 312 , "dismount-failed" , "Drive Broken") ,
      new HsmEvent.ErrorEntry( 400 , "say" ) ,
      new HsmEvent.ErrorEntry( 401 , "say-ok") ,
   
   } ;
   static {
      for( int i = 0 ; i < _errors.length ; i++ )
        __errorHash.put( new Integer(_errors[i].getId() ),_errors[i] ) ;
   }
   private ErrorEntry _entry  = null ;
   private String     _detail = null ;
   private int        _actionId = 0 ;
   public HsmEvent( Object source , int actionId ){
      this( source , actionId , null ) ;
      
   }
   public HsmEvent( Object source , int actionId , String detail ){
      super( source , 0 , "("+actionId+"):"+detail ) ;
      _actionId = actionId ;
      _detail   = detail ;
      _entry    = (ErrorEntry)__errorHash.get( new Integer(actionId) );
   }
   public int getActionId(){ return _actionId ; }
   public int getSectionId(){
      Object source = getSource() ;
      return ( source != null ) && 
             ( source instanceof LSProtocolHandler.Section ) ?
       ((LSProtocolHandler.Section)source).getSectionId() : -1 ;
   }
   public String getCommand(){ 
      return _entry == null ? "Unkown" : _entry.getCommandString(_detail) ; 
   }
   public String getDetail(){
      if( _detail != null )return _detail ;
      String detail = null ;
      if( ( _entry != null ) && ( ( detail = _entry.getDetail() ) != null ) )
         return detail ;
      return "" ;
   }
   public String toString(){return getActionCommand();}
   public String getActionCommand(){
      if( _entry != null ){
          return _entry.toString()+"(detail="+getDetail()+")" ;
      }else{
          return "Unkonw("+_actionId+")"+"(detail="+getDetail()+")" ;
      }
   }
   public boolean isOk(){ return ( _actionId / 10 % 10 ) == 0 ; }
}
