//$Id: PnfsGetChecksumMessage.java,v 1.2 2007-08-30 21:11:07 abaranov Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;
import  diskCacheV111.util.ChecksumPersistence;

//Base class for flag messages to PnfsManager


public class PnfsGetChecksumAllMessage extends PnfsMessage {
   private int[] _value     = null ;

   private static final long serialVersionUID = 8848728352746947853L;

   public PnfsGetChecksumAllMessage( PnfsId pnfsId ){
      super( pnfsId ) ;
      setReplyRequired(true);
   }
   public int[] getValue(){ return _value; }
   public void   setValue(int[] value){ _value = value; }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}
