// $Id: PoolMgrGetPoolByLink.java,v 1.3 2006-08-22 00:11:11 timur Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;

public class PoolMgrGetPoolByLink extends Message {
    private static final long serialVersionUID = 7012732987581691248L;

    //private static final long serialVersionUID = ;

    private String linkName;
    private long   fileSize;
    private String poolName;
    public PoolMgrGetPoolByLink(
                              String linkName){
        this.linkName = linkName;
	setReplyRequired(true);
    }


    public String toString(){
       if( getReturnCode() == 0 ) {
           return "LinkName=" +
                   (linkName == null ? "<unknown>" :
                           linkName);
       } else {
           return super.toString();
       }
    }

    public String getLinkName() {
        return linkName;
    }

    public void setLinkName(String linkName) {
        this.linkName = linkName;
    }

    public void setFilesize( long filesize ){
       this.fileSize = filesize ;
    }

    public long getFilesize(){ return this.fileSize ; }
    public void setFileSize( long fileSize ){
       this.fileSize = fileSize ;
    }

    public long getFileSize(){ return this.fileSize ; }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }
}
