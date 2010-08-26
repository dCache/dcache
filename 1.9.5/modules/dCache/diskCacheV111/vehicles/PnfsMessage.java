//$Id: PnfsMessage.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;
//Base class for messages to PnfsManager


public class PnfsMessage extends Message {

    private PnfsId _pnfsId = null;
    private String _path   = null ;

    private static final long serialVersionUID = -3686370854772807059L;

    public PnfsMessage(String pnfsId){
        this( new PnfsId( pnfsId ) );
    }
    public PnfsMessage(PnfsId pnfsId){
	_pnfsId = pnfsId ;
    }

    public PnfsMessage(){ }

    public void setPnfsPath( String pnfsPath ){ _path = pnfsPath ; }
    public String getPnfsPath(){ return _path ;}

    public PnfsId getPnfsId(){
	return _pnfsId;
    }

    public void setPnfsId(String pnfsId){
	_pnfsId = new PnfsId(pnfsId);
    }
    public void setPnfsId(PnfsId pnfsId){
	_pnfsId = pnfsId ;
    }
    public String toString(){
        return _pnfsId==null?
               (_path==null?"NULL":("Path="+_path)):
               ("PnfsId="+_pnfsId.toString()) ;
    }

    @Override
    public boolean invalidates(Message message)
    {
        if (message instanceof PnfsMessage) {
            PnfsMessage msg = (PnfsMessage) message;
            if (getPnfsId() != null && msg.getPnfsId() != null &&
                !getPnfsId().equals(msg.getPnfsId())) {
                return false;
            }

            if (getPnfsPath() != null && msg.getPnfsPath() != null &&
                !getPnfsPath().equals(msg.getPnfsPath())) {
                return false;
            }
        }
        return true;
    }
}



