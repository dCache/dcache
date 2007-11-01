//$Id: PnfsFlagMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;

//Base class for flag messages to PnfsManager


public class PnfsFlagMessage extends PnfsMessage {
   private String _flagName  = null ;
   private String _operation = null ;
   private String _value     = null ;
   
   private static final long serialVersionUID = 8848728352446647852L;
   
   public PnfsFlagMessage( PnfsId pnfsId , String flag , String operation ){
      super( pnfsId ) ;
      _flagName  = flag ;
      _operation = operation ;
      setReplyRequired(true);
   }
   public String getOperation(){ return _operation ; }
   public String getFlagName(){ return _flagName ; }
   public void setValue( String value ){ _value = value  ; }
   public String getValue(){ return _value ; }
}
