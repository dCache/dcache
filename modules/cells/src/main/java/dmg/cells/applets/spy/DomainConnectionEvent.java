package dmg.cells.applets.spy ;


public class DomainConnectionEvent {

   private Object _source;
   private String _message = "" ;
   
   public DomainConnectionEvent( Object source ){
      _source = source ;
   }
   public DomainConnectionEvent( Object source , String message ){
      _source = source ;
      _message = message ;
   }
   public Object getSource(){ return _source ; }
   public String getMessage(){ return _message ; }

}
