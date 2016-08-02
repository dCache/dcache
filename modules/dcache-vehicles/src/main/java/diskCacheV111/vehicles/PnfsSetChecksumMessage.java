//$Id: PnfsSetChecksumMessage.java,v 1.2 2007-08-30 21:11:07 abaranov Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

//Base class for flag messages to PnfsManager


public class PnfsSetChecksumMessage extends PnfsMessage {
   private final String _value;
   private final int    _type;

   private static final long serialVersionUID = 8848728352746647852L;

   public PnfsSetChecksumMessage( PnfsId pnfsId , int type, String value){
      super( pnfsId ) ;
     _value = value;
     _type  = type;
      setReplyRequired(true);
   }
   public String getValue(){ return _value; }
   public int    getType(){ return _type; }
}
