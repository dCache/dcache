package diskCacheV111.vehicles;

import com.google.common.base.Joiner;

import java.util.Collection;

public class PoolRemoveFilesMessage extends PoolMessage {

    private final String[] _filesList;

    private static final long serialVersionUID = 7090652304453652269L;

    public PoolRemoveFilesMessage(String poolName, Collection<String> files){
        this(poolName, files.toArray(new String[files.size()]));
    }

    public PoolRemoveFilesMessage(String poolName, String... files){
        super(poolName);
        _filesList = files;
        setReplyRequired(true);
    }

    public String[] getFiles() {
         return _filesList;
    }

    public String toString(){
        return super.toString() + ";RemoveFiles=" + Joiner.on(",").join(_filesList);
    }
}
