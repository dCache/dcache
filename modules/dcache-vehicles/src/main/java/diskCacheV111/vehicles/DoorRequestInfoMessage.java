package diskCacheV111.vehicles;

import dmg.cells.nucleus.CellAddressCore;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;

public class DoorRequestInfoMessage extends PnfsFileInfoMessage {

    private long _transactionTime;
    private String _client = "unknown";
    private String _clientChain = "unknown";

    private static final long serialVersionUID = 2469895982145157834L;
    private String _transferPath;

    private MoverInfoMessage moverInfo;

    public DoorRequestInfoMessage(CellAddressCore address) {
        this(address, "request");
    }

    public DoorRequestInfoMessage(CellAddressCore address, String action) {
        super(action, "door", address, null);
    }

    public void setTransactionDuration(long duration) {
        _transactionTime = duration;
    }

    public long getTransactionDuration() {
        return _transactionTime;
    }

    @Override
    public String toString() {
        return "DoorRequestInfoMessage{" +
              "transactionTime=" + _transactionTime +
              ", client='" + _client + '\'' +
              ", clientChain='" + _clientChain + '\'' +
              ", transferPath='" + _transferPath + '\'' +
              "} " + super.toString();
    }

    public String getClient() {
        return _client;
    }

    public void setClient(String client) {
        _client = client;
    }

    public void setClientChain(String chain) {
        _clientChain = chain;
    }

    public String getClientChain() {
        return _clientChain;
    }

    public MoverInfoMessage getMoverInfo() {
        return moverInfo;
    }

    public void setMoverInfo(MoverInfoMessage moverInfo) {
        this.moverInfo = moverInfo;
    }

    public String getOwner() {
        Subject subject = getSubject();
        String owner = Subjects.getDn(subject);
        if (owner == null) {
            owner = Subjects.getUserName(subject);
        }
        return owner;
    }

    public int getGid() {
        Subject subject = getSubject();
        if (subject != null) {
            long[] gids = Subjects.getGids(subject);
            if (gids.length > 0) {
                return (int) gids[0];
            }
        }
        return -1;
    }

    public int getUid() {
        Subject subject = getSubject();
        if (subject != null) {
            long[] uids = Subjects.getUids(getSubject());
            if (uids.length > 0) {
                return (int) uids[0];
            }
        }
        return -1;
    }

    @Override
    public void accept(InfoMessageVisitor visitor) {
        visitor.visit(this);
    }

    public String getTransferPath() {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path) {
        _transferPath = path;
    }
}
