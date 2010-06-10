// $Id: PnfsCreateEntryMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import org.dcache.namespace.FileAttribute;
import diskCacheV111.namespace.NameSpaceProvider;
import java.util.Set;

public class PnfsCreateEntryMessage extends PnfsGetStorageInfoMessage {

    private String      _path        = null;
    private int _uid = NameSpaceProvider.DEFAULT;
    private int _gid = NameSpaceProvider.DEFAULT;
    private int _mode = NameSpaceProvider.DEFAULT;

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

    public PnfsCreateEntryMessage(String path,
            int uid ,
            int gid ,
            int mode,
            Set<FileAttribute> attr){
        super(attr);
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

    @Override
    public boolean invalidates(Message message)
    {
        /* Notice that PnfsCreateEntryMessage inherits from
         * PnfsGetStorageInfoMessage. Therefore we cannot rely on the
         * default implementation in PnfsMessage.
         */
        if (message instanceof PnfsMessage) {
            PnfsMessage msg = (PnfsMessage) message;
            if (getPnfsPath() != null && msg.getPnfsPath() != null &&
                !getPnfsPath().equals(msg.getPnfsPath())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isIdempotent()
    {
        return false;
    }
}
