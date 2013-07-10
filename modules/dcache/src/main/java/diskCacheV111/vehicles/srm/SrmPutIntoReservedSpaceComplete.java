//$Id: SrmPutIntoReservedSpaceComplete.java,v 1.2 2004-11-05 12:07:20 tigran Exp $
// $Log: not supported by cvs2svn $
// Revision 1.1  2003/11/09 19:49:22  cvs
// this message tells the number of bytes utilized from the reserved space
//

package diskCacheV111.vehicles.srm;
//Base class for messages to SRM


public class SrmPutIntoReservedSpaceComplete extends SrmMessage {
    private String poolName;
    private String requestToken;
    private String fileRequestToken;
    private String spaceToken;
    private long storedSize;

    private static final long serialVersionUID = -7108292877799941735L;

    public SrmPutIntoReservedSpaceComplete(
    String poolName,
    String requestToken,
    String fileRequestToken,
    String spaceToken,
    long storedSize) {
        this.poolName = poolName;
        this.requestToken = requestToken;
        this.fileRequestToken=fileRequestToken;
        this.spaceToken = spaceToken;
        this.storedSize = storedSize;
    }

    public String toString(){
        return "SrmPutIntoReservedSpaceComplete, [ "+
        "requestToken="+requestToken+", "+
        "fileRequestToken="+fileRequestToken+", "+
        "spaceToken="+spaceToken+
        "storedSize="+storedSize+
        " ]";
    }

    /** Getter for property requestToken.
     * @return Value of property requestToken.
     *
     */
    public String getRequestToken() {
        return requestToken;
    }

    /** Setter for property requestToken.
     * @param requestToken New value of property requestToken.
     *
     */
    public void setRequestToken(String requestToken) {
        this.requestToken = requestToken;
    }

    /** Getter for property fileRequestToken.
     * @return Value of property fileRequestToken.
     *
     */
    public String getFileRequestToken() {
        return fileRequestToken;
    }

    /** Setter for property fileRequestToken.
     * @param fileRequestToken New value of property fileRequestToken.
     *
     */
    public void setFileRequestToken(String fileRequestToken) {
        this.fileRequestToken = fileRequestToken;
    }

    /** Getter for property spaceToken.
     * @return Value of property spaceToken.
     *
     */
    public String getSpaceToken() {
        return spaceToken;
    }

    /** Setter for property spaceToken.
     * @param spaceToken New value of property spaceToken.
     *
     */
    public void setSpaceToken(String spaceToken) {
        this.spaceToken = spaceToken;
    }

    /** Getter for property storedSize.
     * @return Value of property storedSize.
     *
     */
    public long getStoredSize() {
        return storedSize;
    }

    /** Setter for property storedSize.
     * @param storedSize New value of property storedSize.
     *
     */
    public void setStoredSize(long storedSize) {
        this.storedSize = storedSize;
    }

    /** Getter for property poolName.
     * @return Value of property poolName.
     *
     */
    public String getPoolName() {
        return poolName;
    }

    /** Setter for property poolName.
     * @param poolName New value of property poolName.
     *
     */
    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

}
