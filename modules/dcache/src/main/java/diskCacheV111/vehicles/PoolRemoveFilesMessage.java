package diskCacheV111.vehicles;


public class PoolRemoveFilesMessage extends PoolMessage {
    // this is sent from the LazyCleaner to the pool

    private String _filesList[];

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
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString()).append(";RemoveFiles=");
        if( _filesList != null ) {
            for (String file : _filesList) {
                sb.append(",").append(file);
            }
        }

        return sb.toString();
    }
}
