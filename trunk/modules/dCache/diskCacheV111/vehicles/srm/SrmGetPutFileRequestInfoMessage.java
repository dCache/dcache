//$Id: SrmGetPutFileRequestInfoMessage.java,v 1.3 2004-11-05 12:07:20 tigran Exp $
// $Log: not supported by cvs2svn $
// Revision 1.2  2003/11/09 19:52:53  cvs
// first alfa version of srm v2  space reservation functions is complete
//

package diskCacheV111.vehicles.srm;
import diskCacheV111.vehicles.Message;
//Base class for messages to SRM


public class SrmGetPutFileRequestInfoMessage extends SrmMessage {
    private String username;
    private String protocol;
    private String path;
    private long fileSize;
    private String poolname;
    private String requestToken;
    private String fileRequestToken;
    private String spaceToken;
    
    private static final long serialVersionUID = 5216898530672206672L;
    
    public SrmGetPutFileRequestInfoMessage(String username,String path, String protocol){
        this.username=username;
        this.path=path;
        this.protocol = protocol;
    }
    
    public String toString(){ 
        return "SrmGetPutFileRequestInfoMessage, [ username="+username+
        ", path="+path+", protocol="+protocol+", "+
        "fileSize="+fileSize+", "+
        "poolname="+poolname+", "+
        "requestToken="+requestToken+", "+
        "fileRequestToken="+fileRequestToken+", "+
        "spaceToken="+spaceToken+
        " ]";
    }
    
    /** Getter for property username.
     * @return Value of property username.
     *
     */
    public java.lang.String getUsername() {
        return username;
    }
    
    /** Setter for property username.
     * @param username New value of property username.
     *
     */
    public void setUsername(java.lang.String username) {
        this.username = username;
    }
    
    /** Getter for property protocol.
     * @return Value of property protocol.
     *
     */
    public java.lang.String getProtocol() {
        return protocol;
    }
    
    /** Setter for property protocol.
     * @param protocol New value of property protocol.
     *
     */
    public void setProtocol(java.lang.String protocol) {
        this.protocol = protocol;
    }
    
    /** Getter for property path.
     * @return Value of property path.
     *
     */
    public java.lang.String getPath() {
        return path;
    }
    
    /** Setter for property path.
     * @param path New value of property path.
     *
     */
    public void setPath(java.lang.String path) {
        this.path = path;
    }
    
    /** Getter for property fileSize.
     * @return Value of property fileSize.
     *
     */
    public long getFileSize() {
        return fileSize;
    }
    
    /** Setter for property fileSize.
     * @param fileSize New value of property fileSize.
     *
     */
    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }
    
    /** Getter for property poolname.
     * @return Value of property poolname.
     *
     */
    public java.lang.String getPoolname() {
        return poolname;
    }
    
    /** Setter for property poolname.
     * @param poolname New value of property poolname.
     *
     */
    public void setPoolname(java.lang.String poolname) {
        this.poolname = poolname;
    }
    
    /** Getter for property requestToken.
     * @return Value of property requestToken.
     *
     */
    public java.lang.String getRequestToken() {
        return requestToken;
    }
    
    /** Setter for property requestToken.
     * @param requestToken New value of property requestToken.
     *
     */
    public void setRequestToken(java.lang.String requestToken) {
        this.requestToken = requestToken;
    }
    
    /** Getter for property fileRequestToken.
     * @return Value of property fileRequestToken.
     *
     */
    public java.lang.String getFileRequestToken() {
        return fileRequestToken;
    }
    
    /** Setter for property fileRequestToken.
     * @param fileRequestToken New value of property fileRequestToken.
     *
     */
    public void setFileRequestToken(java.lang.String fileRequestToken) {
        this.fileRequestToken = fileRequestToken;
    }
    
    /** Getter for property spaceToken.
     * @return Value of property spaceToken.
     *
     */
    public java.lang.String getSpaceToken() {
        return spaceToken;
    }
    
    /** Setter for property spaceToken.
     * @param spaceToken New value of property spaceToken.
     *
     */
    public void setSpaceToken(java.lang.String spaceToken) {
        this.spaceToken = spaceToken;
    }
    
}


    
