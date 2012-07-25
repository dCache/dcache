package dmg.cells.services ;

public class SetupInfoMessage implements java.io.Serializable {

   static final long serialVersionUID = -6264137521434235334L;
   private String _action;
   private String _class;
   private Object _payload;
   private String _name;
   
   public SetupInfoMessage( String action ,
                            String setupName ,
                            String setupClass ,
                            Object setupPayload ){
      _payload    = setupPayload ;
      _class      = setupClass ;
      _action     = action ; 
      _name       = setupName ; 
                           
      
   }
   public SetupInfoMessage( String setupName ,
                            String setupClass ){
      this( "get" , setupName , setupClass , null );                      
   }
   public String getAction(){ return _action ; }
   public Object getPayload(){ return _payload ; }
   public String getSetupClass(){ return _class ; }
   public String getSetupName(){ return _name ; }
   public void setPayload( Object payload ){ _payload = payload ; }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append(_action).append(":[");
      if(_name!=null) {
          sb.append(_name);
      }
      sb.append(",");
      if(_class!=null) {
          sb.append(_class);
      }
      sb.append(",");
      if(_payload!=null) {
          sb.append(_payload.getClass().getName());
      }
      sb.append("]");
      return sb.toString();
   }
}
