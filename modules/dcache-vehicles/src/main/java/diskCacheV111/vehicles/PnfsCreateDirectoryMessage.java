package diskCacheV111.vehicles;

import java.util.Set;

import org.dcache.namespace.FileAttribute;

public class PnfsCreateDirectoryMessage extends PnfsCreateEntryMessage {

    private static final long serialVersionUID = 2081981117629353921L;

    public PnfsCreateDirectoryMessage(String path){
        super(path);
    }
    public PnfsCreateDirectoryMessage(String path, int uid, int gid, int mode){
        super(path,uid,gid,mode);
    }
    public PnfsCreateDirectoryMessage(String path,
            int uid,
            int gid,
            int mode,
            Set<FileAttribute> attr){
        super(path,uid,gid,mode,attr);
    }
}
