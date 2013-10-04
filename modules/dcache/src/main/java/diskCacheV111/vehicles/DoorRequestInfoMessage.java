// $Id: DoorRequestInfoMessage.java,v 1.8 2006-04-11 09:47:53 tigran Exp $
package diskCacheV111.vehicles;

import org.stringtemplate.v4.ST;

import javax.security.auth.Subject;

import org.dcache.auth.Subjects;

public class DoorRequestInfoMessage extends PnfsFileInfoMessage
{
    private long _transactionTime;
    private String _client = "unknown";

    private static final long serialVersionUID = 2469895982145157834L;

    public DoorRequestInfoMessage(String cellName) {
        super("request", "door", cellName, null);
    }

    public DoorRequestInfoMessage(String cellName, String action) {
    	super(action, "door", cellName, null);
     }

    public void setTransactionDuration(long duration) {
        _transactionTime = duration;
    }

    public long getTransactionDuration() {
        return _transactionTime;
    }

    public String toString() {
        return getInfoHeader() + " [" + this.getUserInfo() + "] " + getFileInfo() + " " + _transactionTime
                + " " + getTimeQueued() + " " + getResult();
    }

    public String getClient() {
        return _client;
    }

    public void setClient(String client) {
        _client = client;
    }

    public String getOwner()
    {
        Subject subject = getSubject();
        String owner = Subjects.getDn(subject);
        if (owner == null) {
            owner = Subjects.getUserName(subject);
        }
        return owner;
    }

    public int getGid()
    {
        long[] gids = Subjects.getGids(getSubject());
        return (gids.length > 0) ? (int) gids[0] : -1;
    }

    public int getUid()
    {
        long[] uids = Subjects.getUids(getSubject());
        return (uids.length > 0) ? (int) uids[0] : -1;
    }

    public String getUserInfo()
    {
        return "\"" + getOwner() + "\":" + getUid() + ":" + getGid() + ":" + _client;
    }

    @Override
    public void fillTemplate(ST template)
    {
        super.fillTemplate(template);
        template.add("transactionTime", _transactionTime);
        template.add("uid", getUid());
        template.add("gid", getGid());
        template.add("owner", getOwner());
        template.add("client", getClient());
    }
}
