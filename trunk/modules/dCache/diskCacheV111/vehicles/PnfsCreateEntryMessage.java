// $Id: PnfsCreateEntryMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

public class PnfsCreateEntryMessage extends PnfsGetStorageInfoMessage {
    
    private String      _path        = null;
    private int _uid = -1 , _gid = -1 , _mode = 0 ;
    
    private static final long serialVersionUID = -8197311585737333341L;
    
    public PnfsCreateEntryMessage(String path){
	_path = path;
	setReplyRequired(true);
    }
    public PnfsCreateEntryMessage(String path, int uid , int gid , int mode ){
	_path = path;
        _uid  = uid ;
        _gid  = gid ;
        _mode = mode ;
	setReplyRequired(true);
    }
    public String getPath(){
	return _path;
    }
    public int getUid(){ return _uid ; }
    public int getGid(){ return _gid ; }
    public int getMode(){return _mode ; }
    
}
