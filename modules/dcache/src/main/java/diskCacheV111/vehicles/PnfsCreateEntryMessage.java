// $Id: PnfsCreateEntryMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;

import org.dcache.namespace.FileAttribute;

public class PnfsCreateEntryMessage extends PnfsGetStorageInfoMessage {

    private final String _path;
    private final int _uid;
    private final int _gid;
    private final int _mode;

    private static final long serialVersionUID = -8197311585737333341L;

    public PnfsCreateEntryMessage(String path){
        _path = path;
        _uid = NameSpaceProvider.DEFAULT;
        _gid = NameSpaceProvider.DEFAULT;
        _mode = NameSpaceProvider.DEFAULT;
        setPnfsPath(path);
        setReplyRequired(true);
    }
    public PnfsCreateEntryMessage(String path, int uid , int gid , int mode ){
        _path = path;
        _uid  = uid ;
        _gid  = gid ;
        _mode = mode ;
        setPnfsPath(path);
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
        setPnfsPath(path);
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
        return genericInvalidatesForPnfsMessage(message);
    }

    @Override
    public boolean fold(Message message)
    {
        return false;
    }
}
