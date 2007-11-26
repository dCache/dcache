package diskCacheV111.vehicles;
/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class RemoteHttpTransferManagerMessage extends Message 
{
  private long   id;
  private int    uid = 0;
  private int    gid = 0;
  private int bufferSize = 0;
  private int numberOfRetries = 3;
  private int numberOfPerformedRetries = 0;
  
  
  //true is transfer from http server to dcache
  // false if otherwise
  private boolean store; 
  private String httpUrl;
  private String pnfsPath;
  private int returnCode;
  private String description;
  
  private static final long serialVersionUID = -6660118485350265999L;
  
  public RemoteHttpTransferManagerMessage( int uid, 
            int gid, 
            int bufferSize,  
            String httpUrl, 
            String pnfsPath, 
            boolean store,long id)
  {
    this.uid = uid;
    this.gid = gid;
    if(bufferSize <=0)
    {
        throw new IllegalArgumentException(
            "illegal buffer size "+bufferSize);
    }
    this.bufferSize = bufferSize;
    this.httpUrl = httpUrl;
    this.pnfsPath = pnfsPath;
    this.store = store;
    this.id = id;
  }
   
  /** Getter for property uid.
   * @return Value of property uid.
   */
  public int getUid() {
      return uid;
  }
  
  /** Getter for property gid.
   * @return Value of property gid.
   */
  public int getGid() {
      return gid;
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
  
  /** Getter for property httpUrl.
   * @return Value of property httpUrl.
   */
  public java.lang.String getHttpUrl() {
      return httpUrl;
  }
  
  /** Getter for property pnfsPath.
   * @return Value of property pnfsPath.
   */
  public java.lang.String getPnfsPath() {
      return pnfsPath;
  }
  
  /** Getter for property returnCode.
   * @return Value of property returnCode.
   */
  public int getReturnCode() {
      return returnCode;
  }
  
  /** Setter for property returnCode.
   * @param returnCode New value of property returnCode.
   */
  public void setReturnCode(int returnCode) {
      this.returnCode = returnCode;
  }
  
  /** Getter for property description.
   * @return Value of property description.
   */
  public java.lang.String getDescription() {
      return description;
  }
  
  /** Setter for property description.
   * @param description New value of property description.
   */
  public void setDescription(java.lang.String description) {
      this.description = description;
  }
  
  /** Getter for property numberOfRetries.
   * @return Value of property numberOfRetries.
   */
  public int getNumberOfRetries() {
      return numberOfRetries;
  }
  
  /** Setter for property numberOfRetries.
   * @param numberOfRetries New value of property numberOfRetries.
   */
  public void setNumberOfRetries(int numberOfRetries) {
      this.numberOfRetries = numberOfRetries;
  }
  
  /** Getter for property numberOfPerformedRetries.
   * @return Value of property numberOfPerformedRetries.
   */
  public int getNumberOfPerformedRetries() {
      return numberOfPerformedRetries;
  }
  
  public void increaseNumberOfPerformedRetries() {
      this.numberOfPerformedRetries++;
  }
  
  /** Getter for property id.
   * @return Value of property id.
   */
  public long getId() {
      return id;
  }
  
}



