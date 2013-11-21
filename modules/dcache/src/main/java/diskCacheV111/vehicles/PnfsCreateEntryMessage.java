package diskCacheV111.vehicles;

import java.util.Collections;
import java.util.Set;

import org.dcache.namespace.FileAttribute;

import static diskCacheV111.namespace.NameSpaceProvider.DEFAULT;

public class PnfsCreateEntryMessage extends PnfsGetStorageInfoMessage {

    private final String _path;
    private final int _uid;
    private final int _gid;
    private final int _mode;

    private static final long serialVersionUID = -8197311585737333341L;

    public PnfsCreateEntryMessage(String path) {
        this(path, DEFAULT, DEFAULT, DEFAULT);
    }

    public PnfsCreateEntryMessage(String path, Set<FileAttribute> attr)
    {
        this(path, DEFAULT, DEFAULT, DEFAULT, attr);
    }

    public PnfsCreateEntryMessage(String path, int uid, int gid, int mode) {
        this(path, uid, gid, mode, Collections.<FileAttribute>emptySet());
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
