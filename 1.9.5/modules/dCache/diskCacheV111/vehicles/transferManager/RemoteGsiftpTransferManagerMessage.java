package diskCacheV111.vehicles.transferManager;
/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class RemoteGsiftpTransferManagerMessage extends TransferManagerMessage {
    private boolean emode = true;
    private int streams_num = 5;
    private int tcpBufferSize = 0;
    private int bufferSize = 0;
    
    //true is transfer from http server to dcache
    // false if otherwise
    private boolean store;
    private String pnfsPath;
    private String credentialName;

    public RemoteGsiftpTransferManagerMessage(
            String user,
            int uid,
            int gid,
            String gsiftpUrl,
            String pnfsPath,
            boolean store,
            Long credentialId,
            int bufferSize,
            int tcpBufferSize) {
	this(user, uid, gid, gsiftpUrl, pnfsPath, store, credentialId, "Unknown",
             bufferSize, tcpBufferSize);
    }

    public RemoteGsiftpTransferManagerMessage(
            String user,
            int uid,
            int gid,
            String gsiftpUrl,
            String pnfsPath,
            boolean store,
            Long credentialId,
            String credentialName,
            int bufferSize,
            int tcpBufferSize) {
        super(user, uid, gid, pnfsPath,gsiftpUrl, store,credentialId);
        if(bufferSize <=0 ) {
            throw new IllegalArgumentException(
                    "illegal buffer size "+bufferSize);
        }
        this.credentialName = credentialName;
        this.bufferSize = bufferSize;
        this.tcpBufferSize = tcpBufferSize;
        this.pnfsPath = pnfsPath;
        this.store = store;
    }
    
    public RemoteGsiftpTransferManagerMessage(
            String user,
            int uid,
            int gid,
            String gsiftpUrl,
            String pnfsPath,
            boolean store,
            Long credentialId,
            int bufferSize,
            int tcpBufferSize,
            String spaceReservationId,
            boolean spaceReservationStrict,
            Long size) {
        this(user, uid, gid, gsiftpUrl, pnfsPath, store, credentialId, "Unknown",
             bufferSize, tcpBufferSize, spaceReservationId, spaceReservationStrict, size);
    }

    public RemoteGsiftpTransferManagerMessage(
            String user,
            int uid,
            int gid,
            String gsiftpUrl,
            String pnfsPath,
            boolean store,
            Long credentialId,
            String credentialName,
            int bufferSize,
            int tcpBufferSize,
            String spaceReservationId,
            boolean spaceReservationStrict,
            Long size) {
        super(user,
                uid,
                gid,
                pnfsPath,
                gsiftpUrl,
                store,
                credentialId,
                spaceReservationId,
                spaceReservationStrict,
                size);
        if(bufferSize <=0 ) {
            throw new IllegalArgumentException(
                    "illegal buffer size "+bufferSize);
        }
        this.credentialName = credentialName;
        this.bufferSize = bufferSize;
        this.tcpBufferSize = tcpBufferSize;
        this.pnfsPath = pnfsPath;
        this.store = store;
    }
    
    /** Getter for property bufferSize.
     * @return Value of property bufferSize.
     */
    public int getBufferSize() {
        return bufferSize;
    }
    
    /** Getter for property store.
     * @return Value of property store.
     */
    public boolean isStore() {
        return store;
    }
    
    /** Getter for property gsiftpUrl.
     * @return Value of property gsiftpUrl.
     */
    public java.lang.String getGsiftpUrl() {
        return getRemoteURL() ;
    }
    
    /** Getter for property pnfsPath.
     * @return Value of property pnfsPath.
     */
    public java.lang.String getPnfsPath() {
        return pnfsPath;
    }
   
    /** Getter for property credentialName.
     * @return Value of credentialName.
     */
    public String getCredentialName() {
        return credentialName;
    }
 
    /** Getter for property emode.
     * @return Value of property emode.
     */
    public boolean isEmode() {
        return emode;
    }
    
    /** Setter for property emode.
     * @param emode New value of property emode.
     */
    public void setEmode(boolean emode) {
        this.emode = emode;
    }
    
    /** Getter for property streams_num.
     * @return Value of property streams_num.
     */
    public int getStreams_num() {
        return streams_num;
    }
    
    /** Setter for property streams_num.
     * @param streams_num New value of property streams_num.
     */
    public void setStreams_num(int streams_num) {
        this.streams_num = streams_num;
    }
    
    /** Getter for property tcpBufferSize.
     * @return Value of property tcpBufferSize.
     */
    public int getTcpBufferSize() {
        return tcpBufferSize;
    }
    
    /** Setter for property tcpBufferSize.
     * @param tcpBufferSize New value of property tcpBufferSize.
     */
    public void setTcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
    }
    
    /** Setter for property bufferSize.
     * @param bufferSize New value of property bufferSize.
     */
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
    
    
}



