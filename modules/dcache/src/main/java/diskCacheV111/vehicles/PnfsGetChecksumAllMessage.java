package diskCacheV111.vehicles;

import  diskCacheV111.util.PnfsId;

@Deprecated
public class PnfsGetChecksumAllMessage extends PnfsMessage {
   private int[] _value;

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
