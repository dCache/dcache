// $Id: DoorRequestInfoMessage.java,v 1.8 2006-04-11 09:47:53 tigran Exp $
package diskCacheV111.vehicles;

public class DoorRequestInfoMessage extends PnfsFileInfoMessage {
    private long _transactionTime = 0;


    private String _client = "unknown";
    private String _owner = "unknown";

    private int _uid = -1;
    private int _gid = -1;

    private static final long serialVersionUID = 2469895982145157834L;

    public DoorRequestInfoMessage(String cellName) {
        super("request", "door", cellName, null);
    }

    public DoorRequestInfoMessage(String cellName, String action) {
    	//action: "remove"
    	super(action, "door", cellName, null);
     }

    public void setTransactionTime(long transactionTime) {
        _transactionTime = transactionTime;
    }

    public long getTransactionTime() {
        return _transactionTime;
    }

    public void setOwner(String owner) {
       if( owner != null ) {
             _owner = owner;
        }
    }

    public String getOwner() {
        return _owner;
    }

    public String toString() {
        return getInfoHeader() + " " + this.getUserInfo() + " " + getFileInfo() + " " + _transactionTime
                + " " + getTimeQueued() + " " + getResult();
    }

    public int getGid() {
        return _gid;
    }

    public void setGid(int gid) {
        _gid = gid;
    }

    public int getUid() {
        return _uid;
    }

    public void setUid(int uid) {
        _uid = uid;
    }

    public String getClient() {
        return _client;
    }

    public void setClient(String _client) {
        this._client = _client;
    }

    public String getUserInfo() {

        return "[\"" + _owner + "\":" + _uid + ":" + _gid + ":" + _client + "]" ;

    }
}
