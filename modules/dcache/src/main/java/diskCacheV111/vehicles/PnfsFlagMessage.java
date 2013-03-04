//$Id: PnfsFlagMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

//Base class for flag messages to PnfsManager


public class PnfsFlagMessage extends PnfsMessage {

    public enum FlagOperation {
        GET,
        SET,
        REMOVE,
        REPLACE,
        SETNOOVERWRITE
    }

   private final String _flagName ;
   private final FlagOperation _operation ;
   private String _value;

   private static final long serialVersionUID = 8848728352446647852L;

   public PnfsFlagMessage( PnfsId pnfsId , String flag , FlagOperation operation ){
      super( pnfsId ) ;
      _flagName  = flag ;
      _operation = operation ;
      setReplyRequired(true);
   }
   public FlagOperation getOperation(){ return _operation ; }
   public String getFlagName(){ return _flagName ; }
   public void setValue( String value ){ _value = value  ; }
   public String getValue(){ return _value ; }

    @Override
    public boolean invalidates(Message message)
    {
        return super.invalidates(message) && _operation != FlagOperation.GET;
    }
}
