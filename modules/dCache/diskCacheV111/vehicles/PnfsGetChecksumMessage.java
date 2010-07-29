//$Id: PnfsGetChecksumMessage.java,v 1.2 2007-08-30 21:11:07 abaranov Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;

@Deprecated
public class PnfsGetChecksumMessage extends PnfsMessage {
   private String _value     = null ;
   private int    _type      = 0;

   private static final long serialVersionUID = 8848728352746947852L;

   public PnfsGetChecksumMessage( PnfsId pnfsId , int type ){
      super( pnfsId ) ;
     _type  = type;
      setReplyRequired(true);
   }
   public String getValue(){ return _value; }
   public void   setValue(String value){ _value = value; }
   public int    getType(){ return _type; }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}
