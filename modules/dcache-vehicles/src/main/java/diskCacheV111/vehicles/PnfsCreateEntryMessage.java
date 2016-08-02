package diskCacheV111.vehicles;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static org.dcache.namespace.FileAttribute.*;

public class PnfsCreateEntryMessage extends PnfsGetFileAttributes
{
    private final String _path;
    private final int _uid;
    private final int _gid;
    private final int _mode;

    private static final long serialVersionUID = -8197311585737333341L;

    public PnfsCreateEntryMessage(String path) {
        this(path, -1, -1, -1);
    }

    public PnfsCreateEntryMessage(String path, int uid, int gid, int mode) {
        this(path, uid, gid, mode, Collections.emptySet());
    }

    public PnfsCreateEntryMessage(String path,
            int uid,
            int gid,
            int mode,
            Set<FileAttribute> attr) {
        super(path, EnumSet.copyOf(Sets.union(attr,
                EnumSet.of(OWNER, OWNER_GROUP, MODE, TYPE, SIZE,
                        CREATION_TIME, ACCESS_TIME, MODIFICATION_TIME,
                        PNFSID, STORAGEINFO, STORAGECLASS, CACHECLASS, HSM,
                        ACCESS_LATENCY, RETENTION_POLICY))));
        _path = path;
        _uid  = uid ;
        _gid  = gid ;
        _mode = mode ;
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
