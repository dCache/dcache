package diskCacheV111.vehicles;

import java.util.*; 


public class PoolRemoveFilesMessage extends PoolMessage {
    // this is sent from the LazyCleaner to the pool
    
    private String _filesList[] = null;
    
    private static final long serialVersionUID = 7090652304453652269L;
    
    public PoolRemoveFilesMessage(String poolName){
	super(poolName);
	setReplyRequired(true);
    }
    
    public void setFiles(String filesList[]) {
    	_filesList = filesList;
    }
    
    public String[] getFiles() {
         return _filesList;
    }
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append(super.toString()).append(";RemoveFiles=");
        if( _filesList != null )
            for( int i = 0 , n = _filesList.length ; i<n;i++)
                sb.append(",").append(_filesList[i]);
        
        return sb.toString();
    }
}
